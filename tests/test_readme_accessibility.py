"""
Test README.md for accessibility issues.

This test ensures that all images in the README have alt text,
which is required for WCAG 2.1 compliance.
"""

import re
from pathlib import Path


def test_images_have_alt_text():
    """Test that all images in README.md have alt text."""
    readme_path = Path(__file__).parent.parent / "README.md"

    with open(readme_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Find all img tags
    img_pattern = r"<img[^>]*>"
    images = re.findall(img_pattern, content)

    # Check each image has an alt attribute
    images_without_alt = []
    for img in images:
        if "alt=" not in img:
            images_without_alt.append(img)

    assert (
        len(images_without_alt) == 0
    ), f"Found {len(images_without_alt)} images without alt text:\n" + "\n".join(
        images_without_alt
    )


def test_links_with_images_are_accessible():
    """Test that links containing only images have accessible text via alt attributes."""
    readme_path = Path(__file__).parent.parent / "README.md"

    with open(readme_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Find links that contain images
    # Pattern: <a ...><img ...></a> where there's no text between tags
    link_with_img_pattern = r"<a[^>]*>\s*<img[^>]*>\s*</a>"
    links_with_images = re.findall(link_with_img_pattern, content)

    inaccessible_links = []
    for link in links_with_images:
        # Extract the img tag
        img_match = re.search(r"<img[^>]*>", link)
        if img_match:
            img_tag = img_match.group(0)
            # Check if the image has alt text
            if "alt=" not in img_tag:
                inaccessible_links.append(link)

    assert len(inaccessible_links) == 0, (
        f"Found {len(inaccessible_links)} links with images that lack alt text:\n"
        + "\n".join(inaccessible_links)
    )
