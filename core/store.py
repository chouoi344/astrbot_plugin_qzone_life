"""Durable facts and work queues for Qzone Life.

Only this plugin's own state is stored here.  External operations are claimed
before the network call and completed with their follow-up work in one SQLite
transaction.  An interrupted network call is uncertain, never an automatic retry.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


def _json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False
    )


def _copy(value: Any) -> Any:
    return json.loads(_json(value))


class StateStore:
    """Thread-safe, synchronous SQLite store; methods do not perform network I/O."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.execute("PRAGMA busy_timeout = 30000")
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = FULL")
        with self._db:
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS operations (
                    key TEXT PRIMARY KEY, kind TEXT NOT NULL,
                    status TEXT NOT NULL, payload_json TEXT NOT NULL,
                    result_json TEXT, error TEXT, attempts INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS posts (
                    tid TEXT PRIMARY KEY, source TEXT NOT NULL,
                    status TEXT NOT NULL, published_at REAL NOT NULL,
                    local_day TEXT NOT NULL, record_json TEXT NOT NULL,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS posts_quota ON posts(local_day, source);
                CREATE TABLE IF NOT EXISTS comments (
                    key TEXT PRIMARY KEY, tid TEXT NOT NULL REFERENCES posts(tid),
                    status TEXT NOT NULL, created_at REAL NOT NULL,
                    record_json TEXT NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS comments_post ON comments(tid, created_at);
                CREATE TABLE IF NOT EXISTS outbox (
                    event_key TEXT PRIMARY KEY, kind TEXT NOT NULL,
                    entity_id TEXT NOT NULL, payload_json TEXT NOT NULL,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL, error TEXT, result_json TEXT,
                    depends_on TEXT REFERENCES outbox(event_key),
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status, next_attempt_at);
                CREATE INDEX IF NOT EXISTS outbox_entity ON outbox(entity_id, kind);
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id TEXT PRIMARY KEY, day TEXT NOT NULL,
                    schedule_hash TEXT NOT NULL, slot_at REAL NOT NULL,
                    status TEXT NOT NULL, record_json TEXT NOT NULL,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS plans_day ON plans(day, slot_at);
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY, value_json TEXT NOT NULL
                );
                PRAGMA user_version = 1;
            """)

    def close(self) -> None:
        """Close the connection after the plugin's background workers stop."""
        with self._lock:
            self._db.close()

    @staticmethod
    def _operation(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        item = dict(row)
        item["payload"] = json.loads(item.pop("payload_json"))
        raw = item.pop("result_json")
        item["result"] = json.loads(raw) if raw is not None else None
        return item

    @staticmethod
    def _outbox(row: sqlite3.Row | None) -> dict | None:
        return StateStore._operation(row)

    def _post(self, tid: str) -> dict:
        row = self._db.execute("SELECT record_json FROM posts WHERE tid = ?", (tid,)).fetchone()
        if row is None:
            raise KeyError(f"Unknown post: {tid}")
        return json.loads(row["record_json"])

    def _comment(self, key: str) -> dict:
        row = self._db.execute("SELECT record_json FROM comments WHERE key = ?", (key,)).fetchone()
        if row is None:
            raise KeyError(f"Unknown comment: {key}")
        return json.loads(row["record_json"])

    def _claimed(self, key: str, kind: str) -> dict:
        row = self._db.execute("SELECT * FROM operations WHERE key = ?", (key,)).fetchone()
        operation = self._operation(row)
        if operation is None:
            raise KeyError(f"Operation was not claimed: {key}")
        if operation["kind"] != kind:
            raise ValueError(f"Operation {key} is not {kind}")
        if operation["status"] not in ("inflight", "uncertain", "done"):
            raise ValueError(f"Cannot complete operation in {operation['status']} state")
        return operation

    def begin_operation(self, key: str, kind: str, payload: dict) -> dict:
        """Claim a new or explicitly failed action; done/uncertain never reacquire.

        Returns the operation plus ``acquired: bool``.  Keys must name an immutable
        request.  A different kind or payload for an existing key is rejected.
        """
        if not key or not kind:
            raise ValueError("Operation key and kind are required")
        payload_json, now = _json(payload), time.time()
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO operations "
                "(key,kind,status,payload_json,created_at,updated_at) VALUES (?,?,'inflight',?,?,?)",
                (key, kind, payload_json, now, now),
            )
            acquired = bool(self._db.execute("SELECT changes()").fetchone()[0])
            row = self._db.execute("SELECT * FROM operations WHERE key = ?", (key,)).fetchone()
            if row["kind"] != kind or row["payload_json"] != payload_json:
                raise ValueError("An operation key cannot be reused with a different request")
            if row["status"] == "failed":
                self._db.execute(
                    "UPDATE operations SET status='inflight',error=NULL,attempts=attempts+1,"
                    "updated_at=? WHERE key=?",
                    (now, key),
                )
                acquired = True
                row = self._db.execute("SELECT * FROM operations WHERE key=?", (key,)).fetchone()
            result = self._operation(row)
            result["acquired"] = acquired
            return result

    def get_operation(self, key: str) -> dict | None:
        """Return an operation with decoded payload/result, or None."""
        with self._lock:
            return self._operation(
                self._db.execute("SELECT * FROM operations WHERE key=?", (key,)).fetchone()
            )

    def list_operations(self, status: str | None = None, limit: int = 100) -> list[dict]:
        """List recent operations, optionally restricted to one state."""
        sql, args = "SELECT * FROM operations", []
        if status is not None:
            sql += " WHERE status=?"
            args.append(status)
        sql += " ORDER BY created_at DESC,key LIMIT ?"
        args.append(max(0, limit))
        with self._lock:
            return [self._operation(row) for row in self._db.execute(sql, args)]

    def _mark_operation(self, key: str, status: str, error: str) -> None:
        with self._lock, self._db:
            row = self._db.execute("SELECT status FROM operations WHERE key=?", (key,)).fetchone()
            if row is None:
                raise KeyError(key)
            if row[0] == "done":
                return
            self._db.execute(
                "UPDATE operations SET status=?,error=?,updated_at=? WHERE key=?",
                (status, str(error), time.time(), key),
            )

    def mark_operation_uncertain(self, key: str, error: str) -> None:
        """Block automatic retry after an external action may have succeeded."""
        self._mark_operation(key, "uncertain", error)

    def mark_operation_failed(self, key: str, error: str) -> None:
        """Allow retry only after the caller knows the external action did not occur."""
        self._mark_operation(key, "failed", error)

    def _done(self, key: str, result: dict, now: float) -> None:
        self._db.execute(
            "UPDATE operations SET status='done',result_json=?,error=NULL,updated_at=? WHERE key=?",
            (_json(result), now, key),
        )

    def _save_post(self, post: dict, now: float) -> None:
        self._db.execute(
            "INSERT INTO posts(tid,source,status,published_at,local_day,record_json,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(tid) DO UPDATE SET "
            "source=excluded.source,status=excluded.status,published_at=excluded.published_at,"
            "local_day=excluded.local_day,record_json=excluded.record_json,updated_at=excluded.updated_at",
            (
                post["tid"],
                post["source"],
                post["status"],
                post["published_at"],
                post["local_day"],
                _json(post),
                now,
                now,
            ),
        )

    @staticmethod
    def _event_key(kind: str, post: dict, comment_key: str | None = None) -> str:
        suffix = f":{comment_key}" if comment_key is not None else ""
        return f"qzone:{post['binding'].get('bot_id', '')}:{post['tid']}:{kind}{suffix}"

    def _enqueue(self, key: str, kind: str, tid: str, payload: dict, now: float) -> None:
        if self._db.execute("SELECT 1 FROM outbox WHERE event_key=?", (key,)).fetchone():
            return
        previous = None
        if kind == "memory":
            row = self._db.execute(
                "SELECT event_key FROM outbox WHERE kind='memory' AND entity_id=? "
                "ORDER BY rowid DESC LIMIT 1",
                (tid,),
            ).fetchone()
            previous = row[0] if row else None
        self._db.execute(
            "INSERT INTO outbox(event_key,kind,entity_id,payload_json,status,next_attempt_at,"
            "depends_on,created_at,updated_at) VALUES(?,?,?,?,'pending',?,?,?,?)",
            (key, kind, tid, _json(payload), now, previous, now, now),
        )

    def _memory(
        self,
        kind: str,
        post: dict,
        occurred_at: float,
        now: float,
        comment: dict | None = None,
        reply: dict | None = None,
    ) -> None:
        key = self._event_key(kind, post, comment["key"] if comment else None)
        payload = {
            "event_key": key,
            "kind": kind,
            "tid": post["tid"],
            "post": post,
            "binding": post["binding"],
            "occurred_at": occurred_at,
        }
        if comment is not None:
            payload["comment"] = comment
            payload["binding"] = comment.get("binding") or post["binding"]
        if reply is not None:
            payload["reply"] = reply
        self._enqueue(key, "memory", post["tid"], payload, now)

    def complete_publish(self, op_key: str, post: dict) -> dict:
        """Record a successful publication, promoting a remotely imported fact if needed.

        The caller must verify the remote result before completing an uncertain
        action.  Promotion retains later deletion and comment history; its memory
        joins the existing event chain instead of resetting already-recorded work.
        """
        post, now = _copy(post), time.time()
        post["tid"] = str(post["tid"])
        if not post["tid"] or post.get("source") not in ("autonomous", "manual"):
            raise ValueError("Post needs a tid and source autonomous/manual")
        if not math.isfinite(float(post["published_at"])) or not post.get("local_day"):
            raise ValueError("Post needs a finite publication timestamp and local day")
        if not isinstance(post.get("binding"), dict):
            raise ValueError("Post needs its original binding")
        post.setdefault("content", "")
        post.setdefault("image_summary", "")
        post.setdefault("images", [])
        post["status"] = "published"
        with self._lock, self._db:
            operation = self._claimed(op_key, "publish")
            if operation["status"] == "done":
                return _copy(operation["result"])
            existing_row = self._db.execute(
                "SELECT record_json FROM posts WHERE tid=?", (post["tid"],)
            ).fetchone()
            if existing_row is not None:
                existing = json.loads(existing_row[0])
                if existing.get("source") != "external":
                    raise ValueError("Post already belongs to a completed publication")
                for row in self._db.execute(
                    "SELECT result_json FROM operations WHERE kind='publish' AND status='done'"
                ):
                    completed = json.loads(row[0]) if row[0] is not None else {}
                    if str(completed.get("tid", "")) == post["tid"]:
                        raise ValueError("Post already belongs to another completed publication")
                if str((existing.get("binding") or {}).get("bot_id", "")) != str(
                    post["binding"].get("bot_id", "")
                ):
                    raise ValueError("Imported post belongs to a different bot account")
                promoted = {**existing, **post}
                if existing.get("status") == "deleted":
                    promoted["status"] = "deleted"
                    promoted["deleted_at"] = existing.get("deleted_at")
                if "memory_id" in existing:
                    promoted["memory_id"] = existing["memory_id"]
                post = promoted
            self._save_post(post, now)
            self._memory("publish", post, post["published_at"], now)
            if post["status"] == "published":
                self._enqueue(
                    self._event_key("self_like", post),
                    "self_like",
                    post["tid"],
                    {"tid": post["tid"], "binding": post["binding"]},
                    now,
                )
            self._done(op_key, post, now)
            return _copy(post)

    def complete_delete(self, op_key: str, tid: str, deleted_at: float) -> dict:
        """Record successful deletion without removing quota/history; cancel pending likes."""
        now = time.time()
        with self._lock, self._db:
            operation = self._claimed(op_key, "delete")
            if operation["status"] == "done":
                return _copy(operation["result"])
            post = self._post(str(tid))
            if post["status"] != "deleted":
                post.update(status="deleted", deleted_at=deleted_at)
                self._save_post(post, now)
                self._memory("delete", post, deleted_at, now)
            self._db.execute(
                "UPDATE outbox SET status='cancelled',updated_at=? "
                "WHERE entity_id=? AND kind='self_like' AND status IN "
                "('pending','reconcile')",
                (now, str(tid)),
            )
            for row in self._db.execute(
                "SELECT record_json FROM comments WHERE tid=? AND status IN ('pending','failed')",
                (str(tid),),
            ).fetchall():
                comment = json.loads(row[0])
                comment["status"] = "cancelled"
                self._save_comment(comment, now)
            self._done(op_key, post, now)
            return _copy(post)

    def _save_comment(self, comment: dict, now: float) -> None:
        self._db.execute(
            "INSERT INTO comments(key,tid,status,created_at,record_json,updated_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET status=excluded.status,"
            "record_json=excluded.record_json,updated_at=excluded.updated_at",
            (
                comment["key"],
                comment["tid"],
                comment["status"],
                comment["created_at"],
                _json(comment),
                now,
            ),
        )

    def record_comment(self, comment: dict) -> bool:
        """Save an already-authorized comment and memory once; return True only if new."""
        comment, now = _copy(comment), time.time()
        comment["key"], comment["tid"] = str(comment["key"]), str(comment["tid"])
        with self._lock, self._db:
            if self._db.execute("SELECT 1 FROM comments WHERE key=?", (comment["key"],)).fetchone():
                return False
            post = self._post(comment["tid"])
            comment.setdefault("binding", post["binding"])
            comment.setdefault("parent_id", "")
            comment.setdefault("author_name", "")
            comment.setdefault(
                "status", "pending" if post["status"] == "published" else "cancelled"
            )
            self._save_comment(comment, now)
            self._memory("comment", post, comment["created_at"], now, comment=comment)
            return True

    def complete_reply(self, op_key: str, comment_key: str, reply: dict) -> dict:
        """Atomically record an actually sent reply and its separate memory event."""
        reply, now = _copy(reply), time.time()
        with self._lock, self._db:
            operation = self._claimed(op_key, "reply")
            if operation["status"] == "done":
                return _copy(operation["result"])
            comment = self._comment(comment_key)
            if comment["status"] == "replied" and comment.get("reply") != reply:
                raise ValueError("This comment already has a recorded reply")
            comment.update(status="replied", reply=reply)
            self._save_comment(comment, now)
            self._memory(
                "reply",
                self._post(comment["tid"]),
                reply["created_at"],
                now,
                comment=comment,
                reply=reply,
            )
            self._done(op_key, comment, now)
            return _copy(comment)

    def get_post(self, tid: str) -> dict | None:
        """Return a saved publication including any deletion and memory_id fields."""
        with self._lock:
            try:
                return self._post(str(tid))
            except KeyError:
                return None

    def track_post(self, post: dict) -> dict:
        """Import an existing own-space post without asserting this plugin published it.

        Existing local facts win over a later remote scan.  Imported posts use
        source ``external``, do not consume autonomous quota and enqueue no work.
        """
        post, now = _copy(post), time.time()
        post["tid"] = str(post["tid"])
        if not post["tid"] or not isinstance(post.get("binding"), dict):
            raise ValueError("Imported post needs its tid and binding")
        post.update(source="external", status="published")
        post.setdefault("content", "")
        post.setdefault("image_summary", "")
        post.setdefault("images", [])
        post.setdefault("published_at", now)
        post.setdefault("local_day", "")
        with self._lock, self._db:
            row = self._db.execute(
                "SELECT record_json FROM posts WHERE tid=?", (post["tid"],)
            ).fetchone()
            if row is not None:
                return json.loads(row[0])
            self._save_post(post, now)
            return _copy(post)

    def list_posts(self, limit: int = 20, include_deleted: bool = False) -> list[dict]:
        """Return newest publications; deleted facts remain available on request."""
        sql = "SELECT record_json FROM posts"
        if not include_deleted:
            sql += " WHERE status='published'"
        sql += " ORDER BY published_at DESC,tid LIMIT ?"
        with self._lock:
            return [json.loads(row[0]) for row in self._db.execute(sql, (max(0, limit),))]

    def count_autonomous(self, local_day: str) -> int:
        """Count successful autonomous publications, including later-deleted ones."""
        with self._lock:
            return self._db.execute(
                "SELECT COUNT(*) FROM posts WHERE source='autonomous' AND local_day=?", (local_day,)
            ).fetchone()[0]

    def get_comment(self, key: str) -> dict | None:
        """Return a comment, its processing status and actual reply, if present."""
        with self._lock:
            try:
                return self._comment(key)
            except KeyError:
                return None

    def list_comments(
        self, tid: str | None = None, limit: int = 100, status: str | None = None
    ) -> list[dict]:
        """Return recent comments chronologically, optionally across posts/by status."""
        conditions, args = [], []
        if tid is not None:
            conditions.append("tid=?")
            args.append(str(tid))
        if status is not None:
            conditions.append("status=?")
            args.append(status)
        sql = "SELECT record_json FROM comments"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC,key DESC LIMIT ?"
        args.append(max(0, limit))
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
            return [json.loads(row[0]) for row in reversed(rows)]

    def set_comment_status(self, key: str, status: str, error: str | None = None) -> None:
        """Set workflow state without fabricating a reply or memory event."""
        if status not in ("pending", "processing", "failed", "uncertain", "cancelled", "replied"):
            raise ValueError("Invalid comment status")
        with self._lock, self._db:
            comment = self._comment(key)
            if status == "replied" and not comment.get("reply"):
                raise ValueError("Use complete_reply to record a sent reply")
            comment.update(status=status, error=error)
            self._save_comment(comment, time.time())

    def update_post_memory_id(self, tid: str, memory_id: str) -> None:
        """Save the upstream publication-memory identifier after a successful sync."""
        with self._lock, self._db:
            post = self._post(str(tid))
            post["memory_id"] = str(memory_id)
            self._save_post(post, time.time())

    def get_outbox(self, event_key: str) -> dict | None:
        """Return a queue entry, including payload, attempts and predecessor key."""
        with self._lock:
            return self._outbox(
                self._db.execute("SELECT * FROM outbox WHERE event_key=?", (event_key,)).fetchone()
            )

    def due_outbox(
        self, kind: str | None = None, now: float | None = None, limit: int = 20
    ) -> list[dict]:
        """List due pending/reconcile work whose earlier same-post memory is done."""
        now = time.time() if now is None else now
        sql = (
            "SELECT o.* FROM outbox o LEFT JOIN outbox p ON p.event_key=o.depends_on "
            "WHERE o.status IN ('pending','reconcile') AND o.next_attempt_at<=? "
            "AND (o.depends_on IS NULL OR p.status='done')"
        )
        args: list[Any] = [now]
        if kind is not None:
            sql += " AND o.kind=?"
            args.append(kind)
        sql += " ORDER BY o.created_at,o.rowid LIMIT ?"
        args.append(max(0, limit))
        with self._lock:
            return [self._outbox(row) for row in self._db.execute(sql, args)]

    def claim_outbox(self, event_key: str, now: float | None = None) -> bool:
        """Atomically claim due work; callers retain its prior reconcile status."""
        now = time.time() if now is None else now
        with self._lock, self._db:
            cursor = self._db.execute(
                "UPDATE outbox SET status='running',attempts=attempts+1,updated_at=? "
                "WHERE event_key=? AND status IN ('pending','reconcile') AND next_attempt_at<=? "
                "AND (depends_on IS NULL OR EXISTS (SELECT 1 FROM outbox p "
                "WHERE p.event_key=outbox.depends_on AND p.status='done'))",
                (now, event_key, now),
            )
            return cursor.rowcount == 1

    def finish_outbox(self, event_key: str, result: dict | None = None) -> None:
        """Finish work and retain the current publication-memory ID after revisions.

        LivingMemory revisions can replace a record with a new ID.  Its
        ``updated_memory_ids`` map advances our existing publication pointer;
        a deletion/comment event's own ``memory_id`` does not replace that pointer.
        """
        result, now = _copy(result or {}), time.time()
        with self._lock, self._db:
            row = self._db.execute(
                "SELECT * FROM outbox WHERE event_key=?", (event_key,)
            ).fetchone()
            if row is None:
                raise KeyError(event_key)
            if row["status"] == "done":
                return
            if row["status"] != "running":
                raise ValueError("Only claimed outbox work can be completed")
            payload = json.loads(row["payload_json"])
            if row["kind"] == "memory":
                post = self._post(row["entity_id"])
                original_id = post.get("memory_id")
                if payload.get("kind") == "publish" and result.get("memory_id") is not None:
                    post["memory_id"] = str(result["memory_id"])
                replacements = result.get("updated_memory_ids")
                if isinstance(replacements, dict) and post.get("memory_id") is not None:
                    replacements = {
                        str(old): str(new) for old, new in replacements.items() if new is not None
                    }
                    current_id, visited = str(post["memory_id"]), set()
                    while current_id in replacements and current_id not in visited:
                        visited.add(current_id)
                        current_id = replacements[current_id]
                    post["memory_id"] = current_id
                if post.get("memory_id") != original_id:
                    self._save_post(post, now)
            self._db.execute(
                "UPDATE outbox SET status='done',result_json=?,error=NULL,updated_at=? "
                "WHERE event_key=?",
                (_json(result), now, event_key),
            )

    def retry_outbox(
        self, event_key: str, error: str, next_attempt_at: float, needs_reconcile: bool = False
    ) -> None:
        """Requeue claimed work; reconcile requires checking its external event marker."""
        with self._lock, self._db:
            row = self._db.execute(
                "SELECT status FROM outbox WHERE event_key=?", (event_key,)
            ).fetchone()
            if row is None:
                raise KeyError(event_key)
            if row[0] in ("done", "cancelled"):
                return
            self._db.execute(
                "UPDATE outbox SET status=?,error=?,next_attempt_at=?,updated_at=? "
                "WHERE event_key=?",
                (
                    "reconcile" if needs_reconcile else "pending",
                    str(error),
                    next_attempt_at,
                    time.time(),
                    event_key,
                ),
            )

    def save_plan(self, plan: dict) -> dict:
        """Insert an immutable daily opportunity; existing plan IDs keep their state."""
        plan, now = _copy(plan), time.time()
        plan.setdefault("status", "pending")
        plan.setdefault("post_tid", None)
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO plans(plan_id,day,schedule_hash,slot_at,status,"
                "record_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    plan["plan_id"],
                    plan["day"],
                    plan["schedule_hash"],
                    plan["slot_at"],
                    plan["status"],
                    _json(plan),
                    now,
                    now,
                ),
            )
            row = self._db.execute(
                "SELECT record_json FROM plans WHERE plan_id=?", (plan["plan_id"],)
            ).fetchone()
            return json.loads(row[0])

    def list_plans(self, day: str) -> list[dict]:
        """Return persisted opportunities for the supplied local calendar day."""
        with self._lock:
            return [
                json.loads(row[0])
                for row in self._db.execute(
                    "SELECT record_json FROM plans WHERE day=? ORDER BY slot_at,plan_id", (day,)
                )
            ]

    def claim_plan(self, plan_id: str) -> bool:
        """Claim an unexecuted plan once; worker recovery does not reclaim uncertainty."""
        with self._lock, self._db:
            row = self._db.execute(
                "SELECT record_json,status FROM plans WHERE plan_id=?", (plan_id,)
            ).fetchone()
            if row is None or row["status"] != "pending":
                return False
            plan = json.loads(row["record_json"])
            plan["status"] = "running"
            cursor = self._db.execute(
                "UPDATE plans SET status='running',record_json=?,updated_at=? "
                "WHERE plan_id=? AND status='pending'",
                (_json(plan), time.time(), plan_id),
            )
            return cursor.rowcount == 1

    def finish_plan(self, plan_id: str, status: str, post_tid: str | None = None) -> dict:
        """Save a terminal result; cannot silently reset a plan to pending/running."""
        if status not in ("done", "failed", "cancelled", "uncertain", "skipped"):
            raise ValueError("Invalid final plan status")
        with self._lock, self._db:
            row = self._db.execute(
                "SELECT record_json FROM plans WHERE plan_id=?", (plan_id,)
            ).fetchone()
            if row is None:
                raise KeyError(plan_id)
            plan = json.loads(row[0])
            if plan["status"] == "done":
                return plan
            plan.update(status=status, post_tid=str(post_tid) if post_tid is not None else None)
            self._db.execute(
                "UPDATE plans SET status=?,record_json=?,updated_at=? WHERE plan_id=?",
                (status, _json(plan), time.time(), plan_id),
            )
            return _copy(plan)

    def cancel_pending_plans(self, day: str) -> int:
        """Cancel only not-yet-started slots when a day's schedule is replaced."""
        with self._lock, self._db:
            rows = self._db.execute(
                "SELECT plan_id,record_json FROM plans WHERE day=? AND status='pending'", (day,)
            ).fetchall()
            for row in rows:
                plan = json.loads(row["record_json"])
                plan["status"] = "cancelled"
                self._db.execute(
                    "UPDATE plans SET status='cancelled',record_json=?,updated_at=? "
                    "WHERE plan_id=?",
                    (_json(plan), time.time(), row["plan_id"]),
                )
            return len(rows)

    def get_value(self, key: str, default: Any = None) -> Any:
        """Read JSON metadata, such as comment baseline and fixed daily target."""
        with self._lock:
            row = self._db.execute("SELECT value_json FROM kv WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else default

    def set_value(self, key: str, value: Any) -> None:
        """Persist JSON metadata atomically."""
        with self._lock, self._db:
            self._db.execute(
                "INSERT INTO kv(key,value_json) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                (key, _json(value)),
            )

    # Common names for callers that prefer explicit key/value terminology.
    get_kv = get_value
    set_kv = set_value

    def recover_interrupted(self) -> dict:
        """Recover after workers stop: uncertain actions, reconciled memories, safe likes.

        This is explicit rather than a constructor side effect, so merely opening
        a second connection cannot interrupt a live worker.
        """
        now = time.time()
        counts = {"operations": 0, "memory": 0, "self_like": 0, "plans": 0, "comments": 0}
        with self._lock, self._db:
            counts["operations"] = self._db.execute(
                "UPDATE operations SET status='uncertain',error='Interrupted before result was recorded',"
                "updated_at=? WHERE status='inflight'",
                (now,),
            ).rowcount
            for kind, state in (("memory", "reconcile"), ("self_like", "pending")):
                counts[kind] = self._db.execute(
                    "UPDATE outbox SET status=?,next_attempt_at=?,updated_at=? "
                    "WHERE kind=? AND status='running'",
                    (state, now, now, kind),
                ).rowcount
            rows = self._db.execute(
                "SELECT plan_id,record_json FROM plans WHERE status='running'"
            ).fetchall()
            for row in rows:
                plan = json.loads(row["record_json"])
                plan["status"] = "uncertain"
                self._db.execute(
                    "UPDATE plans SET status='uncertain',record_json=?,updated_at=? "
                    "WHERE plan_id=?",
                    (_json(plan), now, row["plan_id"]),
                )
            counts["plans"] = len(rows)
            rows = self._db.execute(
                "SELECT record_json FROM comments WHERE status='processing'"
            ).fetchall()
            for row in rows:
                comment = json.loads(row[0])
                comment["status"] = "uncertain"
                self._save_comment(comment, now)
            counts["comments"] = len(rows)
        return counts

    def summary(self) -> dict:
        """Return per-state counts for posts, comments, operations, outbox and plans."""
        result: dict[str, Any] = {}
        with self._lock:
            for table in ("posts", "comments", "operations", "outbox", "plans"):
                result[table] = {
                    row[0]: row[1]
                    for row in self._db.execute(
                        f"SELECT status,COUNT(*) FROM {table} GROUP BY status"
                    )
                }
            result["outbox_by_kind"] = {}
            for row in self._db.execute(
                "SELECT kind,status,COUNT(*) FROM outbox GROUP BY kind,status"
            ):
                result["outbox_by_kind"].setdefault(row[0], {})[row[1]] = row[2]
        return result
