from wd_notability.web.creations import render_creations_dashboard_html


def test_creations_dashboard_bars_are_forced_left_to_right():
    html = render_creations_dashboard_html().body.decode("utf-8")

    assert ".bar {" in html
    assert "flex-direction: row;" in html
    assert "direction: ltr;" in html
    assert "Quality (low to high)" in html
    assert "id=\"connection-status\"" in html
    assert "Offline" in html
    assert "bucket-partial-weak" in html
    assert "repeating-linear-gradient(\n          135deg" in html
