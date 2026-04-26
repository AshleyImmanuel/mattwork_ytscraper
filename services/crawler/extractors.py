from services.crawler.parsers import safe_text, parse_view_count, parse_duration_text, format_duration

def extract_videos_from_data(data: dict) -> tuple[list[dict], str | None]:
    """
    Parse video/channel info from ytInitialData JSON.
    Returns (list_of_videos, continuation_token_or_None).
    """
    videos = []
    continuation_token = None

    contents = (
        data
        .get("contents", {})
        .get("twoColumnSearchResultsRenderer", {})
        .get("primaryContents", {})
        .get("sectionListRenderer", {})
        .get("contents", [])
    )

    for section in contents:
        cont_renderer = section.get("continuationItemRenderer")
        if cont_renderer:
            continuation_token = (
                cont_renderer
                .get("continuationEndpoint", {})
                .get("continuationCommand", {})
                .get("token")
            )
            continue

        item_section = section.get("itemSectionRenderer", {})
        for item in item_section.get("contents", []):
            video = item.get("videoRenderer")
            if not video:
                continue

            video_id = video.get("videoId", "")
            if not video_id:
                continue

            title = safe_text(video.get("title"))

            channel_name = ""
            channel_id = ""
            owner = video.get("longBylineText") or video.get("ownerText") or video.get("shortBylineText")
            if owner and "runs" in owner:
                runs = owner["runs"]
                if runs:
                    channel_name = runs[0].get("text", "")
                    nav = runs[0].get("navigationEndpoint", {})
                    channel_id = nav.get("browseEndpoint", {}).get("browseId", "")

            view_text = safe_text(video.get("viewCountText"))
            view_count = parse_view_count(view_text)

            duration_text = safe_text(video.get("lengthText"))
            duration_seconds = parse_duration_text(duration_text)

            published_text = safe_text(video.get("publishedTimeText"))

            desc_snippet = safe_text(video.get("detailedMetadataSnippets", [{}])[0].get("snippetText") if video.get("detailedMetadataSnippets") else video.get("descriptionSnippet"))

            badges = video.get("badges", [])
            is_live = any(
                b.get("metadataBadgeRenderer", {}).get("style") == "BADGE_STYLE_TYPE_LIVE_NOW"
                for b in badges
            )
            if is_live:
                continue

            videos.append({
                "videoId": video_id,
                "title": title,
                "channelId": channel_id,
                "channelTitle": channel_name,
                "viewCount": view_count,
                "duration": format_duration(duration_seconds),
                "duration_seconds": duration_seconds,
                "publishedText": published_text,
                "description": desc_snippet,
            })

    return videos, continuation_token

def extract_videos_from_continuation(data: dict) -> tuple[list[dict], str | None]:
    """
    Parse video/channel info from InnerTube continuation response.
    """
    videos = []
    continuation_token = None

    actions = data.get("onResponseReceivedCommands", [])
    for action in actions:
        items = (
            action
            .get("appendContinuationItemsAction", {})
            .get("continuationItems", [])
        )
        for item in items:
            cont_renderer = item.get("continuationItemRenderer")
            if cont_renderer:
                continuation_token = (
                    cont_renderer
                    .get("continuationEndpoint", {})
                    .get("continuationCommand", {})
                    .get("token")
                )
                continue

            video = item.get("videoRenderer")
            if not video:
                section = item.get("itemSectionRenderer", {})
                for sub_item in section.get("contents", []):
                    v = sub_item.get("videoRenderer")
                    if v:
                        process_video_renderer(v, videos)
                continue

            process_video_renderer(video, videos)

    return videos, continuation_token

def process_video_renderer(video: dict, videos: list):
    """Extract a single video from a videoRenderer and append to list."""
    video_id = video.get("videoId", "")
    if not video_id:
        return

    title = safe_text(video.get("title"))

    channel_name = ""
    channel_id = ""
    owner = video.get("longBylineText") or video.get("ownerText") or video.get("shortBylineText")
    if owner and "runs" in owner:
        runs = owner["runs"]
        if runs:
            channel_name = runs[0].get("text", "")
            nav = runs[0].get("navigationEndpoint", {})
            channel_id = nav.get("browseEndpoint", {}).get("browseId", "")

    view_text = safe_text(video.get("viewCountText"))
    view_count = parse_view_count(view_text)

    duration_text = safe_text(video.get("lengthText"))
    duration_seconds = parse_duration_text(duration_text)

    published_text = safe_text(video.get("publishedTimeText"))
    desc_snippet = safe_text(video.get("detailedMetadataSnippets", [{}])[0].get("snippetText") if video.get("detailedMetadataSnippets") else video.get("descriptionSnippet"))

    badges = video.get("badges", [])
    is_live = any(
        b.get("metadataBadgeRenderer", {}).get("style") == "BADGE_STYLE_TYPE_LIVE_NOW"
        for b in badges
    )
    if is_live:
        return

    videos.append({
        "videoId": video_id,
        "title": title,
        "channelId": channel_id,
        "channelTitle": channel_name,
        "viewCount": view_count,
        "duration": format_duration(duration_seconds),
        "duration_seconds": duration_seconds,
        "publishedText": published_text,
        "description": desc_snippet,
    })
