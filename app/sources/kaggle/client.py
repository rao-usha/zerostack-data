"""
Kaggle API client for downloading competition/dataset files.

Official Kaggle API documentation:
https://github.com/Kaggle/kaggle-api

This client wraps the official Kaggle Python API to provide:
- Credential management (env vars or kaggle.json)
- Competition file downloads
- Dataset file downloads
- Extraction of downloaded archives
- Error handling with clear messages

IMPORTANT: Some datasets (like M5) require accepting competition terms first.
Visit the competition page on kaggle.com and accept the rules before downloading.

Rate limits:
- Kaggle API has rate limits but they are generally generous
- We implement conservative defaults to be good citizens
"""

import asyncio
import logging
import os
import zipfile
from pathlib import Path
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class KaggleClient:
    """
    Client for interacting with Kaggle API.

    Responsibilities:
    - Configure Kaggle authentication
    - Download competition files
    - Download dataset files
    - Extract archives
    - Handle errors gracefully

    Note: The Kaggle API is synchronous, so we wrap calls with
    run_in_executor for async compatibility.
    """

    # Known competitions/datasets
    M5_COMPETITION = "m5-forecasting-accuracy"

    def __init__(
        self,
        username: Optional[str] = None,
        key: Optional[str] = None,
        data_dir: str = "./data/kaggle",
    ):
        """
        Initialize Kaggle client.

        Args:
            username: Kaggle username (can also use KAGGLE_USERNAME env var)
            key: Kaggle API key (can also use KAGGLE_KEY env var)
            data_dir: Directory to store downloaded files
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Set environment variables for Kaggle API authentication
        # The kaggle package reads these automatically
        if username:
            os.environ["KAGGLE_USERNAME"] = username
        if key:
            os.environ["KAGGLE_KEY"] = key

        # Lazy import kaggle to avoid issues if not installed
        self._api = None

        logger.info(
            f"Initialized KaggleClient: "
            f"data_dir={self.data_dir}, "
            f"credentials_configured={bool(username and key)}"
        )

    def _get_api(self):
        """
        Get or initialize the Kaggle API instance.

        Returns:
            KaggleApi instance

        Raises:
            ImportError: If kaggle package is not installed
            Exception: If authentication fails
        """
        if self._api is None:
            try:
                from kaggle.api.kaggle_api_extended import KaggleApi

                self._api = KaggleApi()
                self._api.authenticate()
                logger.info("Kaggle API authenticated successfully")
            except ImportError:
                raise ImportError(
                    "kaggle package is not installed. "
                    "Install with: pip install kaggle"
                )
            except Exception as e:
                raise Exception(
                    f"Failed to authenticate with Kaggle API: {e}. "
                    "Ensure KAGGLE_USERNAME and KAGGLE_KEY are set, "
                    "or configure ~/.kaggle/kaggle.json"
                )
        return self._api

    async def download_competition_files(
        self,
        competition: str,
        file_names: Optional[List[str]] = None,
        force: bool = False,
    ) -> Dict[str, Path]:
        """
        Download files from a Kaggle competition.

        Args:
            competition: Competition identifier (e.g., "m5-forecasting-accuracy")
            file_names: Specific files to download. If None, downloads all.
            force: If True, re-download even if files exist

        Returns:
            Dict mapping file names to local paths

        Raises:
            Exception: On download failure
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._download_competition_files_sync, competition, file_names, force
        )

    def _download_competition_files_sync(
        self, competition: str, file_names: Optional[List[str]], force: bool
    ) -> Dict[str, Path]:
        """
        Synchronous implementation of competition file download.
        """
        api = self._get_api()

        # Create competition-specific directory
        comp_dir = self.data_dir / competition
        comp_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = {}

        try:
            # List available files
            logger.info(f"Listing files for competition: {competition}")
            available_files_response = api.competition_list_files(competition)

            # Handle different Kaggle API versions - FileList object vs list
            if hasattr(available_files_response, "files"):
                available_files = available_files_response.files
            elif hasattr(available_files_response, "__iter__"):
                available_files = list(available_files_response)
            else:
                # Try to access as list directly
                available_files = available_files_response

            file_names_list = [
                f.name if hasattr(f, "name") else str(f) for f in available_files
            ]
            logger.info(f"Available files: {file_names_list}")

            # Determine which files to download
            if file_names:
                files_to_download = [
                    f
                    for f in available_files
                    if (f.name if hasattr(f, "name") else str(f)) in file_names
                ]
                if not files_to_download:
                    raise ValueError(
                        f"None of the requested files found: {file_names}. "
                        f"Available: {file_names_list}"
                    )
            else:
                files_to_download = available_files

            # Download each file
            for file_info in files_to_download:
                file_name = (
                    file_info.name if hasattr(file_info, "name") else str(file_info)
                )
                file_size = file_info.size if hasattr(file_info, "size") else "unknown"
                local_path = comp_dir / file_name

                # Check if already exists (respect force flag)
                if local_path.exists() and not force:
                    logger.info(f"File already exists, skipping: {local_path}")
                    downloaded_files[file_name] = local_path
                    continue

                logger.info(f"Downloading: {file_name} ({file_size} bytes)")

                # Download the file
                api.competition_download_file(
                    competition=competition,
                    file_name=file_name,
                    path=str(comp_dir),
                    quiet=False,
                )

                # Check for zip file and extract
                downloaded_path = comp_dir / file_name
                if (
                    downloaded_path.suffix == ".zip"
                    or (comp_dir / f"{file_name}.zip").exists()
                ):
                    zip_path = (
                        comp_dir / f"{file_name}.zip"
                        if (comp_dir / f"{file_name}.zip").exists()
                        else downloaded_path
                    )
                    if zip_path.exists():
                        logger.info(f"Extracting: {zip_path}")
                        with zipfile.ZipFile(zip_path, "r") as zf:
                            zf.extractall(comp_dir)
                        # Update path to extracted file
                        extracted_name = file_name.replace(".zip", "")
                        if (comp_dir / extracted_name).exists():
                            downloaded_path = comp_dir / extracted_name

                downloaded_files[file_name] = downloaded_path
                logger.info(f"Downloaded: {file_name} -> {downloaded_path}")

            return downloaded_files

        except Exception as e:
            # Check for common errors
            error_str = str(e).lower()

            if "403" in error_str or "forbidden" in error_str:
                raise Exception(
                    f"Access forbidden to competition '{competition}'. "
                    "You may need to: "
                    "1) Accept the competition rules at kaggle.com "
                    "2) Check that your API credentials are correct "
                    "3) Verify you have not exceeded API limits"
                )
            elif "404" in error_str or "not found" in error_str:
                raise Exception(
                    f"Competition '{competition}' not found. "
                    "Check the competition name is correct."
                )
            elif "401" in error_str or "unauthorized" in error_str:
                raise Exception(
                    "Kaggle authentication failed. "
                    "Ensure KAGGLE_USERNAME and KAGGLE_KEY are set correctly."
                )
            else:
                raise Exception(f"Failed to download competition files: {e}")

    async def download_m5_files(self, force: bool = False) -> Dict[str, Path]:
        """
        Download M5 Forecasting competition files.

        The M5 dataset includes:
        - sales_train_validation.csv: Historical daily unit sales
        - calendar.csv: Dates, events, SNAP days
        - sell_prices.csv: Prices per store/item/week
        - sample_submission.csv: Submission format
        - sales_train_evaluation.csv: Extended training data

        Args:
            force: If True, re-download even if files exist

        Returns:
            Dict mapping file names to local paths
        """
        # Core files needed for M5 analysis
        m5_files = [
            "sales_train_validation.csv",
            "calendar.csv",
            "sell_prices.csv",
        ]

        logger.info(f"Downloading M5 Forecasting dataset files: {m5_files}")

        return await self.download_competition_files(
            competition=self.M5_COMPETITION, file_names=m5_files, force=force
        )

    def get_local_file_path(self, competition: str, file_name: str) -> Optional[Path]:
        """
        Get the local path for a downloaded file.

        Args:
            competition: Competition identifier
            file_name: File name to look up

        Returns:
            Path to the file if it exists, None otherwise
        """
        # Try with and without .csv extension
        comp_dir = self.data_dir / competition

        candidates = [
            comp_dir / file_name,
            comp_dir / f"{file_name}.csv",
            comp_dir / file_name.replace(".zip", ""),
        ]

        for path in candidates:
            if path.exists():
                return path

        return None

    async def list_competition_files(self, competition: str) -> List[Dict[str, Any]]:
        """
        List available files in a competition.

        Args:
            competition: Competition identifier

        Returns:
            List of file info dictionaries with name, size, etc.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._list_competition_files_sync, competition
        )

    def _list_competition_files_sync(self, competition: str) -> List[Dict[str, Any]]:
        """Synchronous implementation of file listing."""
        api = self._get_api()

        try:
            files_response = api.competition_list_files(competition)

            # Handle different Kaggle API versions
            if hasattr(files_response, "files"):
                files = files_response.files
            elif hasattr(files_response, "__iter__"):
                files = list(files_response)
            else:
                files = files_response

            return [
                {
                    "name": f.name if hasattr(f, "name") else str(f),
                    "size": f.size if hasattr(f, "size") else None,
                    "description": getattr(f, "description", None),
                }
                for f in files
            ]
        except Exception as e:
            logger.error(f"Failed to list competition files: {e}")
            raise
