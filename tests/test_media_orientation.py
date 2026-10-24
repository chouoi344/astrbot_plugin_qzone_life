"""Phone photo orientation is baked into pixels before metadata is discarded."""

import base64
import io
import sys
import unittest
from pathlib import Path

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot_plugin_qzone_life.core.media import read_image


class PhotoOrientationTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_exif_is_removed_while_color_profile_is_preserved(self):
        profile = b"isolated-color-profile-test"
        for image_format in ("JPEG", "PNG"):
            with self.subTest(image_format=image_format):
                exif = Image.Exif()
                exif[274] = 6
                exif[271] = "private-camera"
                exif[306] = "2026:10:25 01:00:00"
                source = io.BytesIO()
                with Image.new("RGB", (20, 10), "red") as photo:
                    photo.save(source, format=image_format, exif=exif, icc_profile=profile)
                with Image.open(io.BytesIO(source.getvalue())) as original:
                    self.assertEqual(original.getexif()[271], "private-camera")
                    self.assertEqual(original.getexif()[306], "2026:10:25 01:00:00")
                result = await read_image(
                    "base64://" + base64.b64encode(source.getvalue()).decode("ascii")
                )
                with Image.open(io.BytesIO(result)) as normalized:
                    self.assertEqual(normalized.size, (10, 20))
                    self.assertFalse(normalized.getexif())
                    self.assertEqual(normalized.info.get("icc_profile"), profile)

    async def test_all_eight_exif_orientations_keep_the_displayed_photo(self):
        colors = {
            "R": (255, 0, 0),
            "G": (0, 255, 0),
            "B": (0, 0, 255),
            "Y": (255, 255, 0),
        }
        corners = {
            1: "RGBY",
            2: "GRYB",
            3: "YBGR",
            4: "BYRG",
            5: "RBGY",
            6: "BRYG",
            7: "YGBR",
            8: "GYRB",
        }
        with Image.new("RGB", (80, 40)) as original:
            for color, box in (
                ("R", (0, 0, 40, 20)),
                ("G", (40, 0, 80, 20)),
                ("B", (0, 20, 40, 40)),
                ("Y", (40, 20, 80, 40)),
            ):
                original.paste(colors[color], box)
            for orientation, expected_corners in corners.items():
                with self.subTest(orientation=orientation):
                    exif = Image.Exif()
                    exif[274] = orientation
                    source = io.BytesIO()
                    original.save(source, format="JPEG", quality=100, subsampling=0, exif=exif)
                    result = await read_image(
                        "base64://" + base64.b64encode(source.getvalue()).decode("ascii")
                    )
                    with Image.open(io.BytesIO(result)) as photo:
                        self.assertEqual(photo.format, "PNG")
                        self.assertEqual(photo.size, (40, 80) if orientation >= 5 else (80, 40))
                        self.assertIn(photo.getexif().get(274), (None, 1))
                        positions = (
                            (5, 5),
                            (photo.width - 6, 5),
                            (5, photo.height - 6),
                            (photo.width - 6, photo.height - 6),
                        )
                        for position, color in zip(positions, expected_corners):
                            self.assertTrue(
                                all(
                                    abs(a - b) <= 3
                                    for a, b in zip(photo.getpixel(position), colors[color])
                                ),
                                (orientation, position, photo.getpixel(position), colors[color]),
                            )
