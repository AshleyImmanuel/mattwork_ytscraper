import asyncio
from services.crawler.parsers import parse_view_count, parse_duration_text

async def extract_videos_from_dom(page) -> list[dict]:
    """
    Parse video information directly from the rendered DOM.
    """
    try:
        videos = await page.evaluate("""
            () => {
                const results = [];
                const renderers = document.querySelectorAll('ytd-video-renderer');
                
                for (const renderer of renderers) {
                    try {
                        const titleEl = renderer.querySelector('#video-title');
                        const title = titleEl ? titleEl.textContent.trim() : '';
                        const href = titleEl ? titleEl.getAttribute('href') : '';
                        const videoId = href ? new URLSearchParams(href.split('?')[1] || '').get('v') || '' : '';
                        
                        const channelEl = renderer.querySelector('#channel-name a, .ytd-channel-name a, #text.ytd-channel-name');
                        const channelName = channelEl ? channelEl.textContent.trim() : '';
                        const channelHref = channelEl ? (channelEl.getAttribute('href') || '') : '';
                        
                        let channelId = '';
                        if (channelHref.includes('/channel/')) {
                            channelId = channelHref.split('/channel/')[1]?.split('/')[0] || '';
                        }
                        
                        const viewsEl = renderer.querySelector('.inline-metadata-item, #metadata-line span');
                        const viewsText = viewsEl ? viewsEl.textContent.trim() : '';
                        
                        const durationEl = renderer.querySelector('ytd-thumbnail-overlay-time-status-renderer span, .ytd-thumbnail-overlay-time-status-renderer');
                        const durationText = durationEl ? durationEl.textContent.trim() : '';
                        
                        const descEl = renderer.querySelector('#description-text, .metadata-snippet-text');
                        const descText = descEl ? descEl.textContent.trim() : '';
                        
                        if (videoId) {
                            results.push({
                                videoId,
                                title,
                                channelId,
                                channelTitle: channelName,
                                viewsText,
                                durationText,
                                description: descText,
                            });
                        }
                    } catch (e) {}
                }
                return results;
            }
        """)

        processed = []
        for v in videos:
            processed.append({
                "videoId": v["videoId"],
                "title": v["title"],
                "channelId": v["channelId"],
                "channelTitle": v["channelTitle"],
                "viewCount": parse_view_count(v.get("viewsText", "")),
                "duration": v.get("durationText", "0:00"),
                "duration_seconds": parse_duration_text(v.get("durationText", "")),
                "publishedText": "",
                "description": v.get("description", ""),
            })
        return processed
    except Exception:
        return []
