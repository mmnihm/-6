import io
import secrets
import string

from PIL import Image, ImageDraw, ImageFont, ImageFilter


ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def generate_captcha_image():
    answer = "".join(secrets.choice(ALPHABET) for _ in range(6))
    width, height = 280, 108
    image = Image.new("RGB", (width, height), (245, 248, 252))
    draw = ImageDraw.Draw(image)

    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", 43)
    except OSError:
        font = ImageFont.load_default(size=43)

    for _ in range(24):
        x1 = secrets.randbelow(width)
        y1 = secrets.randbelow(height)
        x2 = secrets.randbelow(width)
        y2 = secrets.randbelow(height)
        draw.line(
            (x1, y1, x2, y2),
            fill=(150 + secrets.randbelow(70), 155 + secrets.randbelow(65), 165 + secrets.randbelow(60)),
            width=1,
        )

    for index, char in enumerate(answer):
        x = 16 + index * 43 + secrets.randbelow(5)
        y = 24 + secrets.randbelow(12)
        draw.text((x, y), char, font=font, fill=(25, 45, 75))

    for _ in range(180):
        x = secrets.randbelow(width)
        y = secrets.randbelow(height)
        draw.point((x, y), fill=(120, 135, 155))

    image = image.filter(ImageFilter.SMOOTH)
    output = io.BytesIO()
    output.name = "captcha.png"
    image.save(output, format="PNG")
    output.seek(0)
    return output, answer
