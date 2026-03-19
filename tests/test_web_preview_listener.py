from pathlib import Path

from trader.web_preview_listener import parse_posts_from_html


def test_parse_web_preview_html_fixture_extracts_fields() -> None:
    html = Path("tests/fixtures/ivan_preview.html").read_text(encoding="utf-8")
    posts = parse_posts_from_html(html)

    assert len(posts) == 2

    first = posts[0]
    assert first.message_id == 12345
    assert "AKT/USDT" in first.text
    assert first.image_url is None

    latest = posts[-1]
    assert latest.message_id == 12346
    assert "CYBER/USDT" in latest.text
    assert latest.image_url == "https://cdn4.cdn-telegram.org/file/sample_preview.jpg"
