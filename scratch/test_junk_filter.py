import sys
import os

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.utils.extraction import extract_emails_from_text

test_strings = [
    "Contact: business@example.com",
    "Junk image: nba-playoffs-2026@675365183711076-s.png",
    "Another image: @some-icon.jpg",
    "URL with at: https://example.com/tags/news@today?v=123.jpg",
    "Valid obfuscated: contact [at] domain [dot] com",
    "Path noise: user@host/path/to/script.js",
    "Query junk: someone@example.com?v=123",
]

print("Testing Email Extraction Filters...")
for s in test_strings:
    found = extract_emails_from_text(s)
    print(f"Input: {s}")
    print(f"Found: {found}")
    print("-" * 20)

# Assertions
assert "business@example.com" in extract_emails_from_text("business@example.com")
assert "nba-playoffs-2026@675365183711076-s.png" not in extract_emails_from_text("nba-playoffs-2026@675365183711076-s.png")
assert "contact@domain.com" in extract_emails_from_text("contact [at] domain [dot] com")

print("\nAll basic assertions passed!")
