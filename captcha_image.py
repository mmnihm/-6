import io
import secrets

from PIL import Image, ImageDraw, ImageFont


DIGITS = "0123456789"
CODE_LENGTH = 4
OPTION_COUNT = 6
FONT_CANDIDATES = (
    "DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
)


def load_font(size):
    for candidate in FONT_CANDIDATES:
        try:
            return ImageFont.truetype(candidate, size)
        except OSError:
            continue
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def build_captcha():
    """Return (image, options, answer_index) for a pick-one CAPTCHA.

    The image shows a 4-digit code and the keyboard offers that code plus five
    decoys, so a tap is either right or wrong and never ambiguous.
    """
    answer = "".join(secrets.choice(DIGITS) for _ in range(CODE_LENGTH))
    options = [answer]
    while len(options) < OPTION_COUNT:
        candidate = "".join(secrets.choice(DIGITS) for _ in range(CODE_LENGTH))
        if candidate != answer and candidate not in options:
            options.append(candidate)
    answer_index = secrets.randbelow(OPTION_COUNT)
    options[0], options[answer_index] = options[answer_index], options[0]
    return render_code(answer), options, answer_index


def render_code(answer):
    width, height = 320, 132
    image = Image.new("RGB", (width, height), (245, 248, 252))
    draw = ImageDraw.Draw(image)

    # Interference lines stay light and sit behind the digits.
    for _ in range(5):
        draw.line(
            (
                secrets.randbelow(width), secrets.randbelow(height),
                secrets.randbelow(width), secrets.randbelow(height),
            ),
            fill=(198 + secrets.randbelow(20), 205 + secrets.randbelow(18), 214 + secrets.randbelow(16)),
            width=1,
        )

    font = load_font(62)
    step = 68
    left = 32
    for index, char in enumerate(answer):
        x = left + index * step + secrets.randbelow(5)
        y = 30 + secrets.randbelow(11)
        draw.text((x, y), char, font=font, fill=(22, 42, 74))

    for _ in range(90):
        x = secrets.randbelow(width)
        y = secrets.randbelow(height)
        draw.point(
            (x, y),
            fill=(196 + secrets.randbelow(24), 202 + secrets.randbelow(22), 212 + secrets.randbelow(20)),
        )

    output = io.BytesIO()
    output.name = "captcha.png"
    image.save(output, format="PNG")
    output.seek(0)
    return output
