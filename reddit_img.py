# reddit_img.py

import os
import logging
import dropbox
import requests
import requests.auth
import tempfile
import time
from telegram import Bot
from datetime import datetime
from pathlib import Path
import mimetypes
import json
from praw import Reddit
import subprocess
import shutil
import uuid
import websocket
import threading
import io
import re
from pytz import timezone
from datetime import datetime
import random

IST = timezone('Asia/Kolkata')
current_time = datetime.now(IST)

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reddit_upload.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RedditUploader")

# Load secrets from GitHub Actions environment variables
DROPBOX_APP_KEY = os.getenv('DROPBOX_APP_KEY')
DROPBOX_APP_SECRET = os.getenv('DROPBOX_APP_SECRET')
DROPBOX_REFRESH_TOKEN = os.getenv('DROPBOX_REFRESH_TOKEN')
DROPBOX_FOLDER_PATH = os.getenv('DROPBOX_FOLDER_PATH', '/REDDIT_MUL')

REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_REFRESH_TOKEN = os.getenv('REDDIT_REFRESH_TOKEN')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'script v1.0 by u/arulraj_r')
SUBREDDIT_NAME = os.getenv('SUBREDDIT_NAME', 'inkwisp')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Target subreddits for crossposting
TARGET_SUBREDDITS = [
    'motivation',
    'GetMotivated', 
    'selflove',
    'Quotes_Hub',
    'inspirationalquotes',
    'inspiration',
    'Adulting',
    'MotivationalThoughts'
]

# Validate required credentials
required_credentials = {
    'DROPBOX_APP_KEY': DROPBOX_APP_KEY,
    'DROPBOX_APP_SECRET': DROPBOX_APP_SECRET,
    'DROPBOX_REFRESH_TOKEN': DROPBOX_REFRESH_TOKEN,
    'REDDIT_CLIENT_ID': REDDIT_CLIENT_ID,
    'REDDIT_CLIENT_SECRET': REDDIT_CLIENT_SECRET,
    'REDDIT_REFRESH_TOKEN': REDDIT_REFRESH_TOKEN,
    'TELEGRAM_BOT_TOKEN': TELEGRAM_BOT_TOKEN,
    'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID
}

missing_credentials = [k for k, v in required_credentials.items() if not v]
if missing_credentials:
    raise ValueError(f"Missing required credentials: {', '.join(missing_credentials)}")

# === INIT CLIENTS ===
telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)

def get_dropbox_client():
    """Get a fresh Dropbox client with a new access token"""
    try:
        # Get a fresh access token
        url = "https://api.dropbox.com/oauth2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": DROPBOX_REFRESH_TOKEN,
            "client_id": DROPBOX_APP_KEY,
            "client_secret": DROPBOX_APP_SECRET
        }
        response = requests.post(url, data=data)
        response.raise_for_status()
        access_token = response.json().get("access_token")
        
        if not access_token:
            raise Exception("Failed to get Dropbox access token")
            
        # Create and return new client with fresh token
        return dropbox.Dropbox(access_token)
        
    except Exception as e:
        logger.error(f"Failed to get Dropbox client: {e}")
        raise

def get_dropbox_temporary_link(file_path):
    """Get a temporary link for a Dropbox file"""
    try:
        logger.info(f"🔗 Getting temporary link for: {file_path}")
        dbx = get_dropbox_client()
        link = dbx.files_get_temporary_link(file_path)
        logger.info("✅ Got temporary link")
        return link.link
    except Exception as e:
        logger.error(f"❌ Failed to get temporary link: {e}")
        raise

def validate_video(video_path):
    """Validate video file using ffprobe"""
    try:
        # Check if ffprobe is available
        if not shutil.which('ffprobe'):
            logger.warning("⚠️ ffprobe not found, skipping video validation")
            return True

        # Run ffprobe to get video info
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration:stream=codec_type', '-of', 'json', video_path],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"❌ Video validation failed: {result.stderr}")
            return False
            
        # Parse ffprobe output
        info = json.loads(result.stdout)
        
        # Check duration
        duration = float(info['format']['duration'])
        if duration < 2:
            logger.error(f"❌ Video is too short: {duration:.2f}s (minimum 2s)")
            return False
            
        # Check for audio stream
        has_audio = any(stream['codec_type'] == 'audio' for stream in info['streams'])
        if not has_audio:
            logger.error("❌ Video has no audio stream")
            return False
            
        logger.info(f"✅ Video validation successful (duration: {duration:.2f}s, has audio: {has_audio})")
        return True
    except Exception as e:
        logger.error(f"❌ Video validation error: {e}")
        return False

def is_valid_mp4(path):
    """Check if file is a valid MP4 video"""
    try:
        if not shutil.which('ffprobe'):
            logger.warning("⚠️ ffprobe not found, skipping video validation")
            return True

        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 
             'format=duration,size:stream=codec_name,codec_type', 
             '-of', 'json', path],
            capture_output=True,
            text=True,
            check=True
        )
        info = json.loads(result.stdout)
        
        # Basic validation
        duration = float(info['format']['duration'])
        if duration <= 0 or duration > 900:  # Max 15 minutes
            logger.error(f"❌ Invalid duration: {duration}s")
            return False
        
        file_size = int(info['format']['size'])
        if file_size > 1_000_000_000:  # Max 1GB
            logger.error(f"❌ File too large: {file_size} bytes")
            return False
            
        # Check for video stream
        has_video = any(stream['codec_type'] == 'video' for stream in info['streams'])
        if not has_video:
            logger.error("❌ No video stream found")
            return False
        
        logger.info(f"✅ Video validation successful (duration: {duration}s, size: {file_size} bytes)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Video validation failed: {e}")
        return False

def convert_video(input_path, output_path):
    try:
        if not shutil.which('ffmpeg'):
            logger.warning("⚠️ ffmpeg not found, skipping video conversion")
            return input_path

        # Get video dimensions to determine orientation
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'stream=width,height', '-of', 'json', input_path],
            capture_output=True,
            text=True,
            check=True
        )
        info = json.loads(result.stdout)
        width = int(info['streams'][0]['width'])
        height = int(info['streams'][0]['height'])
        
        # Determine target resolution based on orientation
        if width > height:  # Landscape
            scale_filter = 'scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2'
        else:  # Portrait
            scale_filter = 'scale=720:1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2'
        
        logger.info("🔄 Converting video...")
        result = subprocess.run([
            'ffmpeg', '-y',
            '-i', input_path,
            '-vf', scale_filter,
            '-c:v', 'libx264',
            '-profile:v', 'main',
            '-preset', 'medium',
            '-crf', '23',
            '-b:v', '5M',
            '-pix_fmt', 'yuv420p',
            '-c:a', 'aac',
            '-b:a', '128k',
            '-ar', '44100',
            '-strict', '-2',
            '-movflags', '+faststart',
            output_path
        ], capture_output=True, text=True, check=True)
        
        if not is_valid_mp4(output_path):
            raise Exception("Converted video failed validation")
            
        # Log detailed info
        info_result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 
             'format=duration,bit_rate,size:stream=codec_name,codec_type,width,height', 
             '-of', 'json', output_path],
            capture_output=True,
            text=True
        )
        logger.debug(f"Converted video info:\n{json.loads(info_result.stdout)}")
        
        logger.info("✅ Video conversion successful")
        return output_path
    
    except Exception as e:
        logger.error(f"❌ Video conversion failed: {e}")
        return input_path

def verify_token_scopes(token):
    """Verify that the token has all required scopes using PRAW"""
    try:
        # Initialize PRAW client with the token
        reddit = Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            refresh_token=REDDIT_REFRESH_TOKEN,
            user_agent=REDDIT_USER_AGENT
        )
        
        # Get current scopes using PRAW's built-in method
        current_scopes = set(reddit.auth.scopes())
        required_scopes = {'identity', 'submit', 'modposts'}
        
        logger.info(f"🔍 Current token scopes: {current_scopes}")
        
        # Check for missing scopes
        missing_scopes = required_scopes - current_scopes
        if missing_scopes:
            logger.error(f"❌ Missing required scopes: {missing_scopes}")
            raise Exception("Missing required Reddit OAuth scopes")
            
        logger.info(f"✅ Token has required scopes: {current_scopes}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to verify token scopes: {e}")
        return False

def get_reddit_token():
    """Get Reddit access token using refresh token"""
    try:
        auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)
        headers = {
            'User-Agent': REDDIT_USER_AGENT,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': REDDIT_REFRESH_TOKEN
        }
        
        response = requests.post(
            'https://www.reddit.com/api/v1/access_token',
            auth=auth,
            headers=headers,
            data=data,
            timeout=10
        )
        
        response.raise_for_status()
        token_data = response.json()
        
        if 'access_token' not in token_data:
            raise Exception(f"Invalid token response: {token_data}")
            
        return token_data['access_token']
        
    except Exception as e:
        logger.error(f"❌ Failed to refresh token: {e}")
        raise

def get_video_upload_lease(token, file_path):
    """Get S3 upload lease for video"""
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'User-Agent': REDDIT_USER_AGENT,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Get file info
        file_size = os.path.getsize(file_path)
        mime_type = mimetypes.guess_type(file_path)[0] or 'video/mp4'
        filename = os.path.basename(file_path)
        
        # Request upload lease using form data
        data = {
            'filepath': filename,
            'mimetype': mime_type
        }
        
        logger.info("📤 Requesting video upload lease...")
        logger.debug(f"Request headers: {headers}")
        logger.debug(f"Request data: {data}")
        
        response = requests.post(
            'https://oauth.reddit.com/api/video_upload_s3.json',
            headers=headers,
            data=data,  # Use data instead of json for form data
            timeout=30
        )
        
        # Log response for debugging
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        logger.debug(f"Response body: {response.text[:200]}")
        
        response.raise_for_status()
        lease = response.json()
        
        if 'fields' not in lease or 'action' not in lease:
            raise Exception(f"Invalid lease response: {lease}")
            
        logger.info("✅ Got upload lease")
        return lease
        
    except Exception as e:
        logger.error(f"❌ Failed to get upload lease: {e}")
        if hasattr(e, 'response'):
            logger.error(f"Response: {e.response.text[:200]}")
            raise

def get_thumbnail_upload_lease(token, thumbnail_path):
    """Get S3 upload lease for thumbnail"""
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'User-Agent': REDDIT_USER_AGENT,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Get file info
        file_size = os.path.getsize(thumbnail_path)
        mime_type = mimetypes.guess_type(thumbnail_path)[0] or 'image/jpeg'
        filename = os.path.basename(thumbnail_path)
        
        # Request upload lease using form data
        data = {
            'filepath': filename,
            'mimetype': mime_type
        }
        
        logger.info("📤 Requesting thumbnail upload lease...")
        logger.debug(f"Request headers: {headers}")
        logger.debug(f"Request data: {data}")
        
        response = requests.post(
            'https://oauth.reddit.com/api/image_upload_s3.json',
            headers=headers,
            data=data,  # Use data instead of json for form data
            timeout=30
        )
        
        # Log response for debugging
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        logger.debug(f"Response body: {response.text[:200]}")
        
        response.raise_for_status()
        lease = response.json()
        
        if 'fields' not in lease or 'action' not in lease:
            raise Exception(f"Invalid lease response: {lease}")
            
        logger.info("✅ Got thumbnail upload lease")
        return lease
        
    except Exception as e:
        logger.error(f"❌ Failed to get thumbnail upload lease: {e}")
        if hasattr(e, 'response'):
            logger.error(f"Response: {e.response.text[:200]}")
        raise

def upload_to_s3(lease, file_path):
    """Upload file to S3 using lease"""
    try:
        # Prepare form data
        files = {'file': open(file_path, 'rb')}
        data = lease['fields']
        
        logger.info("⬆️ Uploading to S3...")
        response = requests.post(
            lease['action'],
            data=data,
            files=files,
            timeout=120
        )
        response.raise_for_status()
        
        # Extract location from XML response
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.text)
        location = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}Location').text
        
        logger.info("✅ S3 upload successful")
        return location
            
    except Exception as e:
        logger.error(f"❌ Failed to upload to S3: {e}")
        raise

def submit_video_direct(reddit, subreddit, title, video_path, thumbnail_path=None):
    """Submit video directly using Reddit's API"""
    try:
        # Get fresh token
        token = get_reddit_token()
        
        # Get upload lease for video
        video_lease = get_video_upload_lease(token, video_path)
        video_url = upload_to_s3(video_lease, video_path)
        
        # Get upload lease for thumbnail if provided
        video_poster_url = None
        if thumbnail_path:
            thumbnail_lease = get_thumbnail_upload_lease(token, thumbnail_path)
            video_poster_url = upload_to_s3(thumbnail_lease, thumbnail_path)
        
        # Submit the post
        headers = {
            'Authorization': f'Bearer {token}',
            'User-Agent': REDDIT_USER_AGENT,
            'Content-Type': 'application/json'
        }
        
        submit_data = {
            "sr": SUBREDDIT_NAME,
            "kind": "video",
            "title": title,
            "video_url": video_url,
            "api_type": "json"
        }
        
        if video_poster_url:
            submit_data["video_poster_url"] = video_poster_url
        
        logger.info("📤 Submitting post...")
        response = requests.post(
            'https://oauth.reddit.com/api/submit',
            headers=headers,
            json=submit_data,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()["json"]["data"]
        post_href = data["user_submitted_page"]
        
        logger.info(f"✅ Post submitted: {post_href}")
        return post_href
            
    except Exception as e:
        logger.error(f"❌ Direct submission failed: {e}")
        return None

def check_reddit_status():
    try:
        response = requests.get('https://www.redditstatus.com/api/v2/status.json', timeout=10)
        if response.status_code == 200:
            status = response.json()
            if status.get('status', {}).get('indicator') == 'none':
                logger.info("✅ Reddit API status: Operational")
                return True
            else:
                logger.warning(f"⚠️ Reddit API status: {status.get('status', {}).get('description')}")
                return False
        else:
            logger.warning(f"⚠️ Failed to check Reddit API status: {response.status_code}")
            return True  # Proceed anyway
    except Exception as e:
        logger.error(f"❌ Error checking Reddit API status: {e}")
        return True  # Proceed anyway

def generate_thumbnail(video_data):
    """Generate thumbnail from video data in memory"""
    try:
        logger.info("🖼️ Generating thumbnail...")
        
        # Create a temporary file for ffmpeg
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            temp_video.write(video_data.getvalue())
            temp_video_path = temp_video.name
        
        try:
            # Generate thumbnail using ffmpeg
            thumbnail_data = io.BytesIO()
            ffmpeg_cmd = [
                'ffmpeg', '-i', temp_video_path,
                '-ss', '00:00:01',  # Take frame at 1 second
                '-vframes', '1',
                '-f', 'image2pipe',
                '-vcodec', 'mjpeg',
                '-'
            ]
            
            process = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Read thumbnail data
            thumbnail_bytes, _ = process.communicate()
            if process.returncode != 0:
                raise Exception("FFmpeg failed to generate thumbnail")
            
            # Write to BytesIO
            thumbnail_data.write(thumbnail_bytes)
            thumbnail_data.seek(0)
            
            logger.info("✅ Thumbnail generated in memory")
            return thumbnail_data
            
        finally:
            # Clean up temporary video file
            try:
                os.unlink(temp_video_path)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary video: {e}")
                
    except Exception as e:
        logger.error(f"❌ Thumbnail generation failed: {e}")
        return None

def validate_and_convert_video(video_data):
    """Validate video and convert if necessary"""
    try:
        # Create a temporary file for ffprobe
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            temp_video.write(video_data.getvalue())
            temp_video_path = temp_video.name
        
        try:
            # Get video information
            ffprobe_cmd = [
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=duration,size:stream=codec_name,codec_type',
                '-of', 'json',
                temp_video_path
            ]
            
            result = subprocess.run(ffprobe_cmd, capture_output=True, text=True)
            info = json.loads(result.stdout)
            
            duration = float(info['format']['duration'])
            file_size = int(info['format']['size'])
            
            # Check if conversion is needed
            needs_conversion = False
            conversion_params = []
            
            if duration > 840:  # 14 minutes
                logger.warning(f"⚠️ Video duration ({duration}s) exceeds 50 minutes")
                needs_conversion = True
                conversion_params.extend(['-t', '840'])  # Limit to 50 minutes
            
            if file_size > 1_000_000_000:  # 1GB
                logger.warning(f"⚠️ File size ({file_size} bytes) exceeds 1GB")
                needs_conversion = True
                # Calculate target bitrate to get under 1GB
                target_size = 700_000_000  # 700MB to be safe
                target_bitrate = int((target_size * 8) / duration)  # bits per second
                conversion_params.extend(['-b:v', f'{target_bitrate}'])
            
            if needs_conversion:
                logger.info("🔄 Converting video...")
                converted_data = io.BytesIO()
                
                ffmpeg_cmd = [
                    'ffmpeg', '-i', temp_video_path,
                    '-c:v', 'libx264',  # Use H.264 codec
                    '-preset', 'medium',  # Balance between quality and speed
                    '-crf', '23',  # Constant Rate Factor for quality
                    '-c:a', 'aac',  # Use AAC audio codec
                    '-b:a', '128k'  # Audio bitrate
                ] + conversion_params + [
                    '-f', 'mp4',
                    '-movflags', '+faststart',
                    '-'
                ]
                
                process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                # Read converted video data
                converted_bytes, _ = process.communicate()
                if process.returncode != 0:
                    raise Exception("FFmpeg conversion failed")
                
                # Write to BytesIO
                converted_data.write(converted_bytes)
                converted_data.seek(0)
                
                logger.info("✅ Video converted successfully")
                return converted_data
            
            # If no conversion needed, return original data
            video_data.seek(0)
            return video_data
            
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_video_path)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary video: {e}")
                
    except Exception as e:
        logger.error(f"❌ Video validation/conversion failed: {e}")
        return None

def safe_submit_video(subreddit, title, video_path):
    """Safely submit a video to Reddit with proper validation and error handling"""
    try:
        if not is_valid_mp4(video_path):
            raise Exception("Video failed validation before submission")
            
        # Generate thumbnail
        video_data = io.BytesIO()
        with open(video_path, 'rb') as f:
            video_data.write(f.read())
        thumbnail_data = generate_thumbnail(video_data)
        if not thumbnail_data:
            logger.warning("⚠️ Failed to generate thumbnail, proceeding without it")
        
        # First try direct API submission
        logger.info("📤 Attempting direct submission...")
        try:
            post_url = submit_video_direct(subreddit._reddit, subreddit, title, video_path, thumbnail_data)
            if post_url:
                logger.info(f"✅ Direct submission successful: {post_url}")
                return post_url
        except Exception as e:
            logger.warning(f"⚠️ Direct submission failed, falling back to PRAW: {e}")
        
        # Fall back to PRAW submission
        logger.info("📤 Submitting video via PRAW...")
        submission = subreddit.submit_video(
            title=title,
            video_path=video_path,
            thumbnail_path=thumbnail_data,
            without_websockets=True,  # Disable WebSocket to avoid connection issues
            resubmit=True,
            send_replies=True
        )
        
        # Wait for post to appear
        logger.info("⏳ Waiting for post to appear...")
        time.sleep(5)  # Give Reddit time to process
        
        # Look for the new post
        reddit = subreddit._reddit
        submission = None
        
        # Check recent submissions
        logger.info("🔍 Looking for new post...")
        for post in reddit.user.me().submissions.new(limit=10):
            if post.title == title:
                submission = post
                logger.info(f"✅ Found new post: {post.url}")
                break
        
        if not submission:
            logger.error("❌ Could not find new post")
            return None
            
        # Poll for media readiness
        MAX_RETRIES = 15
        RETRY_DELAY = 10
        
        logger.info("⏳ Waiting for media to be ready...")
        for attempt in range(MAX_RETRIES):
            try:
                # Get fresh submission data
                submission = reddit.submission(id=submission.id)
                
                # Check for media
                if not submission.media or "reddit_video" not in submission.media:
                    logger.warning("🪦 Ghost post detected, deleting...")
                    submission.delete()
                    logger.info("♻️ Retrying fresh post...")
                    # Re-submit with thumbnail
                    return safe_submit_video(subreddit, title, video_path)
                
                # Verify video URL
                video_url = submission.secure_media.get("reddit_video", {}).get("fallback_url")
                if video_url:
                    response = requests.head(video_url, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"✅ Video URL verified: {video_url}")
                        return submission
                    else:
                        logger.error(f"❌ Video URL not accessible: {response.status_code}")
                
                logger.info(f"⏳ Waiting for media (attempt {attempt + 1}/{MAX_RETRIES})...")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error checking media status (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                raise
        
        logger.error("❌ Media never became ready")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error during video submission: {e}")
        return None
    finally:
        # Clean up thumbnail if it exists
        if 'thumbnail_data' in locals() and thumbnail_data and isinstance(thumbnail_data, io.BytesIO):
            try:
                thumbnail_data.close()
                logger.debug("Cleaned up thumbnail")
            except Exception as e:
                logger.warning(f"Failed to clean up thumbnail: {e}")

def send_telegram_notification(message: str):
    """Send notification to Telegram"""
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.warning("⚠️ Telegram credentials not configured")
            return
            
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=data)
        response.raise_for_status()
        logger.info("✅ Telegram notification sent")
    except Exception as e:
        logger.error(f"❌ Failed to send Telegram notification: {e}")

def download_to_memory(url, fallback_to_disk=False):
    """Download file to memory with optional fallback to disk"""
    try:
        logger.info(f"⬇️ Downloading from URL: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Create a BytesIO object to store the file in memory
        file_data = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            file_data.write(chunk)
        
        # Reset the pointer to the beginning of the file
        file_data.seek(0)
        logger.info("✅ Downloaded to memory")
        return file_data
    except Exception as e:
        logger.error(f"❌ Memory download failed: {e}")
        if fallback_to_disk:
            try:
                logger.info("🔄 Falling back to disk download...")
                # Create temporary file
                fd, temp_path = tempfile.mkstemp()
                os.close(fd)
                
                # Download to disk
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"✅ Downloaded to disk: {temp_path}")
                return temp_path
            except Exception as disk_error:
                logger.error(f"❌ Disk download also failed: {disk_error}")
                return None
        return None

def find_submission(reddit, title, max_retries=3, delay=5):
    """Find a submission by title in recent posts"""
    for attempt in range(max_retries):
        try:
            # Look in recent submissions
            for post in reddit.user.me().submissions.new(limit=10):
                if post.title == title:
                    logger.info(f"✅ Found submission: {post.id}")
                    return post
            logger.info(f"⏳ Waiting for submission to appear (attempt {attempt + 1}/{max_retries})...")
            time.sleep(delay)
        except Exception as e:
            logger.warning(f"⚠️ Error finding submission (attempt {attempt + 1}): {e}")
            time.sleep(delay)
    return None

def upload_to_reddit(video_url, title):
    """Upload video to Reddit using PRAW's submit_video"""
    temp_video = None
    thumbnail_data = None
    
    try:
        # Initialize PRAW client
        reddit = Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            refresh_token=REDDIT_REFRESH_TOKEN,
            user_agent=REDDIT_USER_AGENT
        )
        
        # Get subreddit
        subreddit = reddit.subreddit(SUBREDDIT_NAME)
        
        # Download video to memory
        video_data = download_to_memory(video_url)
        if not video_data:
            raise Exception("Failed to download video")
        
        # Validate and convert video if needed
        processed_video = validate_and_convert_video(video_data)
        if not processed_video:
            raise Exception("Video validation/conversion failed")
        
        # Generate thumbnail in memory
        thumbnail_data = generate_thumbnail(processed_video)
        if not thumbnail_data:
            raise Exception("Failed to generate thumbnail")
        
        # Create temporary files for PRAW upload
        fd, temp_video = tempfile.mkstemp(suffix=".mp4")
        os.close(fd)
        
        fd, temp_thumbnail = tempfile.mkstemp(suffix=".jpg")
        os.close(fd)
        
        # Write data to temporary files
        with open(temp_video, 'wb') as f:
            f.write(processed_video.getvalue())
        
        with open(temp_thumbnail, 'wb') as f:
            f.write(thumbnail_data.getvalue())
        
        # Submit video using PRAW
        submission = subreddit.submit_video(
            title=title,
            video_path=temp_video,
            thumbnail_path=temp_thumbnail,
            without_websockets=True,
            resubmit=True,
            send_replies=True
        )
        
        # Wait for initial processing
        time.sleep(10)
        
        # Try to find the submission if not returned directly
        if not submission:
            submission = find_submission(reddit, title)
            if not submission:
                raise Exception("Could not find submission after posting")
        
        # Wait for video processing
        max_retries = 6
        retry_delay = 10
        
        for attempt in range(max_retries):
            try:
                # Refresh submission data
                submission = reddit.submission(id=submission.id)
                
                # Check if video is ready
                if hasattr(submission, 'media') and submission.media and 'reddit_video' in submission.media:
                    return f"https://reddit.com{submission.permalink}"
                
                logger.info(f"⏳ Waiting for video processing (attempt {attempt + 1}/{max_retries})...")
                time.sleep(retry_delay)
                
            except Exception as e:
                logger.warning(f"⚠️ Error checking video status: {e}")
                time.sleep(retry_delay)
        
        # If we get here, the video was posted but might still be processing
        return f"https://reddit.com{submission.permalink}"
        
    except Exception as e:
        logger.error(f"❌ Reddit upload failed: {e}")
        raise
        
    finally:
        # Clean up temporary files
        try:
            if temp_video and os.path.exists(temp_video):
                os.remove(temp_video)
            if temp_thumbnail and os.path.exists(temp_thumbnail):
                os.remove(temp_thumbnail)
        except Exception as e:
            logger.warning(f"Failed to clean up temporary files: {e}")

def upload_image_to_reddit(image_url, title):
    """Upload image to Reddit using PRAW"""
    temp_image = None
    
    try:
        # Initialize PRAW client
        reddit = Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            refresh_token=REDDIT_REFRESH_TOKEN,
            user_agent=REDDIT_USER_AGENT
        )
        
        # Get subreddit
        subreddit = reddit.subreddit(SUBREDDIT_NAME)
        
        # Download image (to memory or disk)
        image_data = download_to_memory(image_url)
        if not image_data:
            raise Exception("Failed to download image")
        
        # Handle both memory and disk downloads
        if isinstance(image_data, io.BytesIO):
            # Create temporary file for PRAW
            fd, temp_image = tempfile.mkstemp(suffix=".jpg")
            os.close(fd)
            with open(temp_image, 'wb') as f:
                f.write(image_data.getvalue())
        else:
            # Already downloaded to disk
            temp_image = image_data
        
        # Submit image
        submission = subreddit.submit_image(
            title=title,
            image_path=temp_image,
            send_replies=True
        )
        
        # Wait for post to appear
        time.sleep(5)
        
        # Try to find the submission if not returned directly
        if not submission:
            submission = find_submission(reddit, title)
            if not submission:
                raise Exception("Could not find submission after posting")
        
        return f"https://reddit.com{submission.permalink}"
            
    except Exception as e:
        logger.error(f"❌ Image upload failed: {e}")
        raise
        
    finally:
        # Clean up temporary file
        if temp_image and os.path.exists(temp_image):
            try:
                os.remove(temp_image)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary image: {e}")

def crosspost_to_subreddits(reddit, original_submission, target_subs, custom_title=None):
    """Crosspost to multiple subreddits"""
    try:
        logger.info("🔄 Starting crossposting process...")
        
        # Get the submission object if we have a URL
        if isinstance(original_submission, str):
            # Extract submission ID from URL
            if '/comments/' in original_submission:
                submission_id = original_submission.split('/comments/')[1].split('/')[0]
            else:
                # Try to get from user's recent submissions
                for post in reddit.user.me().submissions.new(limit=5):
                    if post.url == original_submission or f"https://reddit.com{post.permalink}" == original_submission:
                        submission_id = post.id
                        break
                else:
                    raise Exception("Could not find submission ID from URL")
        else:
            submission_id = original_submission.id
        
        # Get the submission object
        submission = reddit.submission(id=submission_id)
        
        # Use custom title or original title
        title = custom_title or submission.title
        
        # Add x-post prefix if not already present
        if not title.lower().startswith('x-post') and not title.lower().startswith('crosspost'):
            title = f"x-post: {title}"
        
        successful_crossposts = []
        failed_crossposts = []
        
        for sub in target_subs:
            try:
                logger.info(f"📤 Crossposting to r/{sub}...")
                
                # Crosspost to target subreddit
                crosspost = submission.crosspost(subreddit=sub, title=title)
                
                # Wait a bit between crossposts to avoid rate limiting
                time.sleep(6)
                
                successful_crossposts.append(sub)
                logger.info(f"✅ Successfully crossposted to r/{sub}")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Failed to crosspost to r/{sub}: {error_msg}")
                failed_crossposts.append((sub, error_msg))
                
                # Continue with other subreddits even if one fails
                continue
        
        # Log summary
        logger.info(f"📊 Crossposting Summary:")
        logger.info(f"✅ Successful: {len(successful_crossposts)} subreddits")
        logger.info(f"❌ Failed: {len(failed_crossposts)} subreddits")
        
        if successful_crossposts:
            logger.info(f"✅ Crossposted to: {', '.join(successful_crossposts)}")
        
        if failed_crossposts:
            logger.info(f"❌ Failed subreddits:")
            for sub, error in failed_crossposts:
                logger.info(f"   - r/{sub}: {error}")
        
        return {
            'successful': successful_crossposts,
            'failed': failed_crossposts,
            'total_attempted': len(target_subs)
        }
        
    except Exception as e:
        logger.error(f"❌ Crossposting process failed: {e}")
        return {
            'successful': [],
            'failed': [(sub, str(e)) for sub in target_subs],
            'total_attempted': len(target_subs)
        }

def get_dropbox_report():
    """Get report of files in Dropbox folder"""
    try:
        dbx = get_dropbox_client()
        result = dbx.files_list_folder(DROPBOX_FOLDER_PATH)
        all_files = result.entries

        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            all_files.extend(result.entries)

        video_files = [f for f in all_files if f.name.lower().endswith(('.mp4', '.mov'))]
        image_files = [f for f in all_files if f.name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif'))]
        other_files = [f for f in all_files if f not in video_files and f not in image_files]

        return {
            'total_files': len(all_files),
            'video_files': len(video_files),
            'image_files': len(image_files),
            'other_files': len(other_files),
            'file_list': {
                'videos': [f.name for f in video_files],
                'images': [f.name for f in image_files],
                'others': [f.name for f in other_files]
            }
        }

    except Exception as e:
        logger.error(f"❌ Failed to get Dropbox report: {e}")
        return None

def send_dropbox_report(report, is_final=False):
    """Send Dropbox report to Telegram"""
    try:
        report_type = "Final" if is_final else "Initial"
        message = f"\n📊 {report_type} Dropbox Report:\n"
        message += f"📁 Total files: {report['total_files']}\n"
        message += f"🎥 Video files: {report['video_files']}\n"
        message += f"🖼️ Image files: {report['image_files']}\n"
        message += f"📄 Other files: {report['other_files']}\n"
        
        # File counts only (no names)
        message += f"\n🔢 Breakdown:\n"
        message += f"🎥 Videos: {len(report['file_list']['videos'])}\n"
        message += f"🖼️ Images: {len(report['file_list']['images'])}\n"
        message += f"📄 Others: {len(report['file_list']['others'])}"
        
        send_telegram_notification(message)
    except Exception as e:
        logger.error(f"❌ Failed to send Dropbox report: {e}")

def clean_filename(filename):
    """Clean filename by removing special characters and numbers in parentheses"""
    try:
        # Remove file extension
        name, ext = os.path.splitext(filename)
        
        # Remove numbers and special characters in parentheses
        # Remove (number) patterns
        name = re.sub(r'\(\d+\)', '', name)
        # Replace underscores with spaces first
        name = name.replace('_', ' ')
        # Remove any remaining special characters
        name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
        # Remove extra spaces
        name = ' '.join(name.split())
        
        return name + ext
    except Exception as e:
        logger.error(f"❌ Failed to clean filename: {e}")
        return filename

def generate_post_title(filename: str) -> str:
    """Generate post title from filename"""
    try:
        # Clean the filename first
        clean_name = clean_filename(filename)
        # Remove file extension
        name_without_ext = os.path.splitext(clean_name)[0]
        # Take first 200 characters and ensure proper spacing
        title = name_without_ext[:200].strip()
        # Log the original and cleaned title for debugging
        logger.info(f"Original filename: {filename}")
        logger.info(f"Cleaned title: {title}")
        return title
    except Exception as e:
        logger.error(f"❌ Failed to generate title: {e}")
        return filename[:200].strip()

def list_dropbox_files():
    """Get all eligible media files from Dropbox folder"""
    try:
        dbx = get_dropbox_client()
        all_files = []

        result = dbx.files_list_folder(DROPBOX_FOLDER_PATH)
        all_files.extend(result.entries)

        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            all_files.extend(result.entries)

        supported_extensions = ('.mp4', '.mov', '.jpg', '.jpeg', '.png', '.gif')
        return [f for f in all_files if f.name.lower().endswith(supported_extensions)]

    except Exception as e:
        logger.error(f"Dropbox list error: {e}")
        return []
        
def get_subreddit_posts(subreddit, limit=10):
    # Get a fresh access token
    token = get_reddit_token()
    headers = {
        'Authorization': f'bearer {token}',
        'User-Agent': REDDIT_USER_AGENT
    }
    url = f'https://oauth.reddit.com/r/{subreddit}/hot'
    params = {'limit': limit}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def main():
    """Main function to process one video from Dropbox"""
    try:
        # Get initial Dropbox report
        initial_report = get_dropbox_report()
        if initial_report:
            logger.info("\n📊 Initial Dropbox Report:")
            logger.info(f"📁 Total files: {initial_report['total_files']}")
            logger.info(f"🎥 Video files: {initial_report['video_files']}")
            logger.info(f"🖼️ Image files: {initial_report['image_files']}")
            logger.info(f"📄 Other files: {initial_report['other_files']}\n")
            # Send initial report to Telegram
            send_dropbox_report(initial_report)
        
        # Get list of files from Dropbox
        files = list_dropbox_files()
        if not files:
            logger.info("No files found in Dropbox folder")
            send_telegram_notification("📭 No files found in Dropbox folder")
            return
        
        # Process only the first file
        file = random.choice(files)
        try:
            # Get temporary link
            temp_link = get_dropbox_temporary_link(file.path_display)
            if not temp_link:
                return
            
            # Generate title from filename
            title = generate_post_title(file.name)
            logger.info(f"📝 Final title for Reddit: {title}")
            
            # Determine file type and upload
            if file.name.lower().endswith(('.mp4', '.mov')):
                url = upload_to_reddit(temp_link, title)
            elif file.name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                url = upload_image_to_reddit(temp_link, title)
            else:
                logger.warning(f"⚠️ Unsupported file type: {file.name}")
                return
            
            if url:
                # Send notification
                send_telegram_notification(f"✅ Successfully uploaded to Reddit: {url}")
                
                # Initialize Reddit client for crossposting
                try:
                    reddit = Reddit(
                        client_id=REDDIT_CLIENT_ID,
                        client_secret=REDDIT_CLIENT_SECRET,
                        refresh_token=REDDIT_REFRESH_TOKEN,
                        user_agent=REDDIT_USER_AGENT
                    )
                    
                    # Crosspost to target subreddits
                    logger.info("🔄 Starting crossposting to motivation subreddits...")
                    crosspost_result = crosspost_to_subreddits(
                        reddit=reddit,
                        original_submission=url,
                        target_subs=TARGET_SUBREDDITS,
                        custom_title=title
                    )
                    
                    # Send crossposting summary to Telegram
                    if crosspost_result['successful']:
                        crosspost_message = f"🔄 Crossposting Summary:\n"
                        crosspost_message += f"✅ Successful: {len(crosspost_result['successful'])} subreddits\n"
                        crosspost_message += f"✅ Crossposted to: {', '.join(crosspost_result['successful'])}\n"
                        
                        if crosspost_result['failed']:
                            crosspost_message += f"❌ Failed: {len(crosspost_result['failed'])} subreddits\n"
                            for sub, error in crosspost_result['failed'][:3]:  # Show first 3 failures
                                crosspost_message += f"   - r/{sub}: {error[:50]}...\n"
                        
                        send_telegram_notification(crosspost_message)
                        logger.info("✅ Crossposting completed")
                    else:
                        send_telegram_notification("❌ Crossposting failed for all subreddits")
                        logger.error("❌ Crossposting failed")
                        
                except Exception as e:
                    logger.error(f"❌ Crossposting process failed: {e}")
                    send_telegram_notification(f"❌ Crossposting failed: {str(e)[:100]}...")
                
                # Delete from Dropbox
                try:
                    dbx = get_dropbox_client()
                    dbx.files_delete_v2(file.path_display)
                    logger.info(f"🗑️ Deleted from Dropbox: {file.name}")
                except Exception as e:
                    logger.error(f"❌ Failed to delete from Dropbox: {file.name} — {e}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process {file.name}: {e}")
            return
        
        # Get final Dropbox report
        final_report = get_dropbox_report()
        if final_report:
            logger.info("\n📊 Final Dropbox Report:")
            logger.info(f"📁 Total files: {final_report['total_files']}")
            logger.info(f"🎥 Video files: {final_report['video_files']}")
            logger.info(f"🖼️ Image files: {final_report['image_files']}")
            logger.info(f"📄 Other files: {final_report['other_files']}\n")
            
            # Send final report to Telegram
            send_dropbox_report(final_report, is_final=True)
            
            # Calculate processed files
            if initial_report:
                processed = {
                    'total': initial_report['total_files'] - final_report['total_files'],
                    'videos': initial_report['video_files'] - final_report['video_files'],
                    'images': initial_report['image_files'] - final_report['image_files'],
                    'others': initial_report['other_files'] - final_report['other_files']
                }
                
                # Send processing summary to Telegram
                summary = "\n📊 Processing Summary:\n"
                summary += f"✅ Total files processed: {processed['total']}\n"
                summary += f"✅ Videos processed: {processed['videos']}\n"
                summary += f"✅ Images processed: {processed['images']}\n"
                summary += f"✅ Other files processed: {processed['others']}\n"
                send_telegram_notification(summary)
                
                logger.info("📊 Processing Summary:")
                logger.info(f"✅ Total files processed: {processed['total']}")
                logger.info(f"✅ Videos processed: {processed['videos']}")
                logger.info(f"✅ Images processed: {processed['images']}")
                logger.info(f"✅ Other files processed: {processed['others']}\n")
        
        # Exit after processing one file
        logger.info("✅ Script completed processing one file")
        send_telegram_notification("✅ Script completed processing one file")
        return
    
    except Exception as e:
        logger.error(f"❌ Main process failed: {e}")
        send_telegram_notification(f"❌ Script failed: {e}")
        raise

if __name__ == "__main__":
    main()
