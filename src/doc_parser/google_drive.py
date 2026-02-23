"""Google Drive API v3: list files + download."""

from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from google.oauth2.service_account import Credentials as SACredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request as AuthRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from doc_parser.config import Settings

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

SUPPORTED_MIMES = {
    "application/pdf",
    "image/png",
    "image/jpeg",
    "image/tiff",
    "image/bmp",
    "image/webp",
}


@dataclass
class DriveFile:
    """Metadata for a file in Google Drive."""

    file_id: str
    name: str
    mime_type: str
    size: int
    created_time: datetime | None
    parents: list[str]


def _get_credentials(settings: Settings) -> Credentials | SACredentials:
    """Build Google credentials from settings."""
    creds_file = settings.google_credentials_file

    if settings.google_service_account:
        return SACredentials.from_service_account_file(creds_file, scopes=SCOPES)

    # OAuth2 interactive flow with cached token
    token_path = Path("token.json")
    creds: Credentials | None = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(AuthRequest())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())

    return creds


def _build_service(settings: Settings):
    """Build a Google Drive API v3 service."""
    creds = _get_credentials(settings)
    return build("drive", "v3", credentials=creds)


class GoogleDriveClient:
    """Client for Google Drive file listing and download."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service = None

    def _get_service(self):
        if self._service is None:
            self._service = _build_service(self._settings)
        return self._service

    def list_files_sync(self, folder_id: str) -> list[DriveFile]:
        """List supported files in a Drive folder (synchronous)."""
        service = self._get_service()
        mime_filter = " or ".join(f"mimeType='{m}'" for m in SUPPORTED_MIMES)
        query = f"'{folder_id}' in parents and ({mime_filter}) and trashed=false"

        results: list[DriveFile] = []
        page_token = None

        while True:
            resp = (
                service.files()
                .list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, size, createdTime, parents)",
                    pageSize=100,
                    pageToken=page_token,
                )
                .execute()
            )

            for f in resp.get("files", []):
                results.append(
                    DriveFile(
                        file_id=f["id"],
                        name=f["name"],
                        mime_type=f["mimeType"],
                        size=int(f.get("size", 0)),
                        created_time=datetime.fromisoformat(f["createdTime"]) if f.get("createdTime") else None,
                        parents=f.get("parents", []),
                    )
                )

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        logger.info("Found %d supported files in folder %s", len(results), folder_id)
        return results

    def download_file_sync(self, file_id: str, dest_path: Path) -> Path:
        """Download a file from Drive to a local path (synchronous)."""
        service = self._get_service()
        request = service.files().get_media(fileId=file_id)

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug("Download %s: %d%%", file_id, int(status.progress() * 100))

        logger.info("Downloaded %s → %s", file_id, dest_path)
        return dest_path

    def get_file_metadata_sync(self, file_id: str) -> DriveFile:
        """Get metadata for a single file (synchronous)."""
        service = self._get_service()
        f = (
            service.files()
            .get(fileId=file_id, fields="id, name, mimeType, size, createdTime, parents")
            .execute()
        )
        return DriveFile(
            file_id=f["id"],
            name=f["name"],
            mime_type=f["mimeType"],
            size=int(f.get("size", 0)),
            created_time=datetime.fromisoformat(f["createdTime"]) if f.get("createdTime") else None,
            parents=f.get("parents", []),
        )

    # Async wrappers for use in the pipeline

    async def list_files(self, folder_id: str) -> list[DriveFile]:
        return await asyncio.to_thread(self.list_files_sync, folder_id)

    async def download_file(self, file_id: str, dest_path: Path) -> Path:
        return await asyncio.to_thread(self.download_file_sync, file_id, dest_path)

    async def get_file_metadata(self, file_id: str) -> DriveFile:
        return await asyncio.to_thread(self.get_file_metadata_sync, file_id)
