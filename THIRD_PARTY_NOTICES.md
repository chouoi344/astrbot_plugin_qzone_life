# Third-party notices

The QQ Space protocol implementation in `core/transport.py` and the helpers in
`vendor/qzone_protocol.py` are adapted from **Zhalslar/astrbot_plugin_qzone**,
version **v3.1.3**, distributed under the **GNU General Public License v3.0**.

- Upstream: https://github.com/Zhalslar/astrbot_plugin_qzone
- Referenced files: `core/qzone/api.py`, `client.py`, `session.py`, `model.py`,
  `parser.py`, `utils.py`, and `core/model.py`.
- The source snapshot was supplied by the user as `astrbot_plugin_qzone-main.zip`
  (SHA-256 `ca5cc14f66ce3c4bddac99b4af10dbadf818d13091c431082e86aa4a36c91edb`).
- Copyright in upstream contributions remains with their respective authors.
- The full upstream license is included in `LICENSE`.
- `logo.png` is reused unchanged from the same supplied source archive and
  remains a QQ Space identifier; it does not imply endorsement by Tencent.

This derivative keeps only the required QQ Space operations. It adds strict
account binding, explicit uncertain-result handling, strict image loading,
plain-dictionary data contracts, and tests. It removes implicit retry/fallback,
AstrBot model dependencies, and unrelated visitor/feed rendering features.
The original reference files in the workspace have not been modified.

Life Scheduler, Gitee AI Image, and LivingMemory are external runtime plugins.
Their implementation is not bundled by this transport module.
