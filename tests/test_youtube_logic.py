import importlib
import os
import unittest
from datetime import datetime
from unittest.mock import patch


def load_youtube_module(env_overrides: dict[str, str]):
    with patch.dict(os.environ, env_overrides, clear=False):
        import services.youtube as youtube

        return importlib.reload(youtube)


class FixedDateTime:
    fixed_now = datetime(2026, 4, 8, 15, 30, 45)

    @classmethod
    def utcnow(cls):
        return cls.fixed_now


class YouTubeLogicTests(unittest.TestCase):
    def test_normalize_region_code_uses_env_mapping_and_default(self):
        youtube = load_youtube_module(
            {
                "YOUTUBE_DEFAULT_REGION": "CA",
                "YOUTUBE_REGION_MAP": "EU:DE,UK:GB,JP:JP",
            }
        )

        self.assertEqual(youtube._normalize_region_code("eu"), "DE")
        self.assertEqual(youtube._normalize_region_code("UK"), "GB")
        self.assertEqual(youtube._normalize_region_code("unknown"), "CA")
        self.assertEqual(youtube._normalize_region_code(""), "CA")

    def test_normalize_region_code_falls_back_to_builtin_defaults(self):
        youtube = load_youtube_module(
            {
                "YOUTUBE_DEFAULT_REGION": "",
                "YOUTUBE_REGION_MAP": "",
            }
        )

        self.assertEqual(youtube._normalize_region_code("UK"), "GB")
        self.assertEqual(youtube._normalize_region_code("anything-else"), "US")

    def test_parse_duration_handles_common_iso8601_shapes(self):
        youtube = load_youtube_module(
            {
                "YOUTUBE_DEFAULT_REGION": "",
                "YOUTUBE_REGION_MAP": "",
            }
        )

        self.assertEqual(youtube._parse_duration("PT59S"), "0:59")
        self.assertEqual(youtube._parse_duration("PT4M3S"), "4:03")
        self.assertEqual(youtube._parse_duration("PT1H2M3S"), "1:02:03")
        self.assertEqual(youtube._parse_duration("PT2H"), "2:00:00")
        self.assertEqual(youtube._parse_duration(""), "0:00")
        self.assertEqual(youtube._parse_duration("not-a-duration"), "0:00")

    def test_date_filter_uses_calendar_boundaries_and_rejects_invalid_values(self):
        youtube = load_youtube_module(
            {
                "YOUTUBE_DEFAULT_REGION": "",
                "YOUTUBE_REGION_MAP": "",
            }
        )

        with patch.object(youtube, "datetime", FixedDateTime):
            self.assertEqual(youtube._date_filter_to_rfc3339("Today"), "2026-04-08T00:00:00Z")
            self.assertEqual(youtube._date_filter_to_rfc3339("This Week"), "2026-04-06T00:00:00Z")
            self.assertEqual(youtube._date_filter_to_rfc3339("Last Month"), "2026-03-01T00:00:00Z")
            self.assertEqual(youtube._date_filter_to_rfc3339("This Year"), "2026-01-01T00:00:00Z")

            with self.assertRaisesRegex(ValueError, "Unsupported date filter"):
                youtube._date_filter_to_rfc3339("Not A Real Filter")

    def test_filter_results_filters_ranges_and_dedupes_channels(self):
        youtube = load_youtube_module(
            {
                "YOUTUBE_DEFAULT_REGION": "",
                "YOUTUBE_REGION_MAP": "",
                "YOUTUBE_ALLOWED_COUNTRIES_BOTH": "US,GB",
                "YOUTUBE_ALLOWED_COUNTRIES_US": "US",
                "YOUTUBE_ALLOWED_COUNTRIES_UK": "GB",
            }
        )

        videos = [
            {
                "videoId": "keep-1",
                "title": "Keep me",
                "channelId": "channel-1",
                "channelTitle": "Channel 1",
                "publishedAt": "2026-04-01",
                "region": "US",
            },
            {
                "videoId": "duplicate-channel",
                "title": "Duplicate channel",
                "channelId": "channel-1",
                "channelTitle": "Channel 1 second video",
                "publishedAt": "2026-04-02",
                "region": "US",
            },
            {
                "videoId": "too-few-views",
                "title": "Low views",
                "channelId": "channel-2",
                "channelTitle": "Channel 2",
                "publishedAt": "2026-04-03",
                "region": "US",
            },
            {
                "videoId": "too-many-views",
                "title": "High views",
                "channelId": "channel-3",
                "channelTitle": "Channel 3",
                "publishedAt": "2026-04-04",
                "region": "US",
            },
            {
                "videoId": "too-few-subs",
                "title": "Low subs",
                "channelId": "channel-4",
                "channelTitle": "Channel 4",
                "publishedAt": "2026-04-05",
                "region": "US",
            },
            {
                "videoId": "too-many-subs",
                "title": "High subs",
                "channelId": "channel-5",
                "channelTitle": "Channel 5",
                "publishedAt": "2026-04-06",
                "region": "US",
            },
        ]

        video_details = {
            "keep-1": {"viewCount": 1500, "likes": 10, "duration": "4:03"},
            "duplicate-channel": {"viewCount": 2000, "likes": 12, "duration": "5:00"},
            "too-few-views": {"viewCount": 999, "likes": 1, "duration": "1:00"},
            "too-many-views": {"viewCount": 5001, "likes": 2, "duration": "2:00"},
            "too-few-subs": {"viewCount": 1500, "likes": 3, "duration": "3:00"},
            "too-many-subs": {"viewCount": 1500, "likes": 4, "duration": "6:00"},
        }

        channel_details = {
            "channel-1": {
                "subscriberCount": 2000,
                "channelUrl": "https://www.youtube.com/channel/channel-1",
                "description": "Accepted channel",
                "country": "US",
            },
            "channel-2": {
                "subscriberCount": 2000,
                "channelUrl": "https://www.youtube.com/channel/channel-2",
                "description": "Low view channel",
                "country": "US",
            },
            "channel-3": {
                "subscriberCount": 2000,
                "channelUrl": "https://www.youtube.com/channel/channel-3",
                "description": "High view channel",
                "country": "US",
            },
            "channel-4": {
                "subscriberCount": 999,
                "channelUrl": "https://www.youtube.com/channel/channel-4",
                "description": "Low sub channel",
                "country": "US",
            },
            "channel-5": {
                "subscriberCount": 5001,
                "channelUrl": "https://www.youtube.com/channel/channel-5",
                "description": "High sub channel",
                "country": "US",
            },
        }

        results = youtube.filter_results(
            videos=videos,
            video_details=video_details,
            channel_details=channel_details,
            min_views=1000,
            max_views=5000,
            min_subs=1000,
            max_subs=5000,
            region_req="US",
        )

        self.assertEqual([row["id"] for row in results], ["keep-1"])
        self.assertEqual(results[0]["channelId"], "channel-1")
        self.assertEqual(results[0]["viewCount"], 1500)
        self.assertEqual(results[0]["numberOfSubscribers"], 2000)

    def test_filter_results_enforces_country_allowlist_and_country_mapping(self):
        youtube = load_youtube_module(
            {
                "YOUTUBE_DEFAULT_REGION": "",
                "YOUTUBE_REGION_MAP": "",
                "YOUTUBE_ALLOWED_COUNTRIES_BOTH": "US,GB",
                "YOUTUBE_ALLOWED_COUNTRIES_US": "US",
                "YOUTUBE_ALLOWED_COUNTRIES_UK": "GB",
            }
        )

        videos = [
            {
                "videoId": "us-video",
                "title": "US video",
                "channelId": "us-channel",
                "channelTitle": "US Channel",
                "publishedAt": "2026-04-01",
                "region": "US",
            },
            {
                "videoId": "gb-video",
                "title": "GB video",
                "channelId": "gb-channel",
                "channelTitle": "GB Channel",
                "publishedAt": "2026-04-02",
                "region": "UK",
            },
            {
                "videoId": "blank-country-video",
                "title": "Blank country video",
                "channelId": "blank-channel",
                "channelTitle": "Blank Channel",
                "publishedAt": "2026-04-03",
                "region": "UK",
            },
            {
                "videoId": "blocked-country-video",
                "title": "Blocked country video",
                "channelId": "blocked-channel",
                "channelTitle": "Blocked Channel",
                "publishedAt": "2026-04-04",
                "region": "US",
            },
        ]

        video_details = {
            "us-video": {"viewCount": 2000, "likes": 5, "duration": "4:00"},
            "gb-video": {"viewCount": 2000, "likes": 6, "duration": "5:00"},
            "blank-country-video": {"viewCount": 2000, "likes": 7, "duration": "6:00"},
            "blocked-country-video": {"viewCount": 2000, "likes": 8, "duration": "7:00"},
        }

        channel_details = {
            "us-channel": {
                "subscriberCount": 3000,
                "channelUrl": "https://www.youtube.com/channel/us-channel",
                "description": "US channel",
                "country": "US",
            },
            "gb-channel": {
                "subscriberCount": 3000,
                "channelUrl": "https://www.youtube.com/channel/gb-channel",
                "description": "GB channel",
                "country": "GB",
            },
            "blank-channel": {
                "subscriberCount": 3000,
                "channelUrl": "https://www.youtube.com/channel/blank-channel",
                "description": "No country channel",
                "country": "",
            },
            "blocked-channel": {
                "subscriberCount": 3000,
                "channelUrl": "https://www.youtube.com/channel/blocked-channel",
                "description": "Blocked country channel",
                "country": "IN",
            },
        }

        both_results = youtube.filter_results(
            videos=videos,
            video_details=video_details,
            channel_details=channel_details,
            min_views=1000,
            max_views=5000,
            min_subs=1000,
            max_subs=5000,
            region_req="Both",
        )

        self.assertEqual(
            [row["id"] for row in both_results],
            ["us-video", "gb-video", "blank-country-video"],
        )
        self.assertEqual(both_results[0]["Country"], "US")
        self.assertEqual(both_results[1]["Country"], "UK")
        self.assertEqual(both_results[2]["Country"], "UK")

        us_only_results = youtube.filter_results(
            videos=videos,
            video_details=video_details,
            channel_details=channel_details,
            min_views=1000,
            max_views=5000,
            min_subs=1000,
            max_subs=5000,
            region_req="ZZ",
        )

        self.assertEqual([row["id"] for row in us_only_results], ["us-video", "blank-country-video"])
        self.assertNotIn("gb-video", [row["id"] for row in us_only_results])
        self.assertNotIn("blocked-country-video", [row["id"] for row in us_only_results])


if __name__ == "__main__":
    unittest.main()
