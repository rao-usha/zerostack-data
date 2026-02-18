"""
GitHub Analytics Service.

Processes and stores GitHub organization and repository metrics.
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.github.client import GitHubClient

logger = logging.getLogger(__name__)


class GitHubAnalyticsService:
    """
    Service for analyzing GitHub organizations and repositories.
    """

    def __init__(self, db: Session):
        self.db = db
        self.client = GitHubClient()
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS github_organizations (
            id SERIAL PRIMARY KEY,
            login VARCHAR(100) NOT NULL UNIQUE,
            name VARCHAR(255),
            description TEXT,
            blog VARCHAR(500),
            location VARCHAR(255),
            email VARCHAR(255),
            twitter_username VARCHAR(100),
            public_repos INTEGER,
            public_gists INTEGER,
            followers INTEGER,
            following INTEGER,
            github_created_at TIMESTAMP,
            github_updated_at TIMESTAMP,
            total_stars INTEGER DEFAULT 0,
            total_forks INTEGER DEFAULT 0,
            total_contributors INTEGER DEFAULT 0,
            velocity_score INTEGER,
            primary_languages JSONB,
            top_repos JSONB,
            last_fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS github_repositories (
            id SERIAL PRIMARY KEY,
            github_id BIGINT UNIQUE,
            org_login VARCHAR(100),
            name VARCHAR(255) NOT NULL,
            full_name VARCHAR(500) NOT NULL UNIQUE,
            description TEXT,
            homepage VARCHAR(500),
            language VARCHAR(100),
            languages JSONB,
            stars INTEGER DEFAULT 0,
            forks INTEGER DEFAULT 0,
            watchers INTEGER DEFAULT 0,
            open_issues INTEGER DEFAULT 0,
            size_kb INTEGER,
            default_branch VARCHAR(100),
            is_fork BOOLEAN DEFAULT FALSE,
            is_archived BOOLEAN DEFAULT FALSE,
            is_private BOOLEAN DEFAULT FALSE,
            topics JSONB,
            license_name VARCHAR(100),
            github_created_at TIMESTAMP,
            github_updated_at TIMESTAMP,
            pushed_at TIMESTAMP,
            last_fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS github_activity_snapshots (
            id SERIAL PRIMARY KEY,
            org_login VARCHAR(100) NOT NULL,
            snapshot_date DATE NOT NULL,
            commits_count INTEGER DEFAULT 0,
            prs_opened INTEGER DEFAULT 0,
            prs_merged INTEGER DEFAULT 0,
            issues_opened INTEGER DEFAULT 0,
            issues_closed INTEGER DEFAULT 0,
            contributors_active INTEGER DEFAULT 0,
            total_stars INTEGER DEFAULT 0,
            total_forks INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(org_login, snapshot_date)
        );

        CREATE TABLE IF NOT EXISTS github_contributors (
            id SERIAL PRIMARY KEY,
            org_login VARCHAR(100) NOT NULL,
            repo_full_name VARCHAR(500),
            username VARCHAR(100) NOT NULL,
            avatar_url VARCHAR(500),
            contributions INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(org_login, repo_full_name, username)
        );

        CREATE INDEX IF NOT EXISTS idx_gh_org_login ON github_organizations(login);
        CREATE INDEX IF NOT EXISTS idx_gh_repo_org ON github_repositories(org_login);
        CREATE INDEX IF NOT EXISTS idx_gh_repo_stars ON github_repositories(stars DESC);
        CREATE INDEX IF NOT EXISTS idx_gh_activity_org ON github_activity_snapshots(org_login);
        CREATE INDEX IF NOT EXISTS idx_gh_contrib_org ON github_contributors(org_login);
        """
        try:
            self.db.execute(text(create_tables_sql))
            self.db.commit()
            logger.info("GitHub tables ready")
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    async def fetch_organization(self, org: str) -> Optional[Dict[str, Any]]:
        """
        Fetch and store organization data with all metrics.

        Args:
            org: Organization login name

        Returns:
            Organization data with computed metrics
        """
        # Get org details
        org_data = await self.client.get_organization(org)
        if not org_data:
            return None

        # Get all repos (paginated)
        all_repos = []
        page = 1
        while True:
            repos = await self.client.get_org_repos(org, page=page)
            if not repos:
                break
            all_repos.extend(repos)
            if len(repos) < 100:
                break
            page += 1

        # Calculate aggregates
        total_stars = sum(r.get("stargazers_count", 0) for r in all_repos)
        total_forks = sum(r.get("forks_count", 0) for r in all_repos)

        # Get top repos by stars
        sorted_repos = sorted(
            all_repos, key=lambda r: r.get("stargazers_count", 0), reverse=True
        )
        top_repos = [r["name"] for r in sorted_repos[:10]]

        # Get primary languages
        language_counts = {}
        for repo in all_repos:
            lang = repo.get("language")
            if lang:
                language_counts[lang] = language_counts.get(lang, 0) + 1
        primary_languages = sorted(
            language_counts.keys(), key=lambda l: language_counts[l], reverse=True
        )[:5]

        # Store repos
        for repo in all_repos:
            await self._store_repository(org, repo)

        # Calculate velocity score
        velocity_score = await self._calculate_velocity_score(org, all_repos)

        # Store organization
        org_record = {
            "login": org_data.get("login"),
            "name": org_data.get("name"),
            "description": org_data.get("description"),
            "blog": org_data.get("blog"),
            "location": org_data.get("location"),
            "email": org_data.get("email"),
            "twitter_username": org_data.get("twitter_username"),
            "public_repos": org_data.get("public_repos"),
            "public_gists": org_data.get("public_gists"),
            "followers": org_data.get("followers"),
            "following": org_data.get("following"),
            "github_created_at": org_data.get("created_at"),
            "github_updated_at": org_data.get("updated_at"),
            "total_stars": total_stars,
            "total_forks": total_forks,
            "velocity_score": velocity_score,
            "primary_languages": primary_languages,
            "top_repos": top_repos,
        }

        self._store_organization(org_record)

        return {
            **org_record,
            "metrics": {
                "total_stars": total_stars,
                "total_forks": total_forks,
                "repo_count": len(all_repos),
                "top_repos": top_repos,
                "primary_languages": primary_languages,
            },
            "velocity_score": velocity_score,
        }

    async def _store_repository(self, org: str, repo: Dict):
        """Store a repository record."""
        insert_sql = text("""
            INSERT INTO github_repositories (
                github_id, org_login, name, full_name, description, homepage,
                language, stars, forks, watchers, open_issues, size_kb,
                default_branch, is_fork, is_archived, is_private, topics,
                license_name, github_created_at, github_updated_at, pushed_at,
                last_fetched_at
            ) VALUES (
                :github_id, :org_login, :name, :full_name, :description, :homepage,
                :language, :stars, :forks, :watchers, :open_issues, :size_kb,
                :default_branch, :is_fork, :is_archived, :is_private, :topics,
                :license_name, :github_created_at, :github_updated_at, :pushed_at,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (full_name) DO UPDATE SET
                stars = EXCLUDED.stars,
                forks = EXCLUDED.forks,
                watchers = EXCLUDED.watchers,
                open_issues = EXCLUDED.open_issues,
                pushed_at = EXCLUDED.pushed_at,
                last_fetched_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
        """)

        try:
            self.db.execute(
                insert_sql,
                {
                    "github_id": repo.get("id"),
                    "org_login": org,
                    "name": repo.get("name"),
                    "full_name": repo.get("full_name"),
                    "description": repo.get("description"),
                    "homepage": repo.get("homepage"),
                    "language": repo.get("language"),
                    "stars": repo.get("stargazers_count", 0),
                    "forks": repo.get("forks_count", 0),
                    "watchers": repo.get("watchers_count", 0),
                    "open_issues": repo.get("open_issues_count", 0),
                    "size_kb": repo.get("size", 0),
                    "default_branch": repo.get("default_branch"),
                    "is_fork": repo.get("fork", False),
                    "is_archived": repo.get("archived", False),
                    "is_private": repo.get("private", False),
                    "topics": repo.get("topics", []),
                    "license_name": repo.get("license", {}).get("name")
                    if repo.get("license")
                    else None,
                    "github_created_at": repo.get("created_at"),
                    "github_updated_at": repo.get("updated_at"),
                    "pushed_at": repo.get("pushed_at"),
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Failed to store repo {repo.get('full_name')}: {e}")
            self.db.rollback()

    def _store_organization(self, org: Dict):
        """Store organization record."""
        import json

        insert_sql = text("""
            INSERT INTO github_organizations (
                login, name, description, blog, location, email,
                twitter_username, public_repos, public_gists, followers,
                following, github_created_at, github_updated_at, total_stars,
                total_forks, velocity_score, primary_languages, top_repos,
                last_fetched_at
            ) VALUES (
                :login, :name, :description, :blog, :location, :email,
                :twitter_username, :public_repos, :public_gists, :followers,
                :following, :github_created_at, :github_updated_at, :total_stars,
                :total_forks, :velocity_score, :primary_languages, :top_repos,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (login) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                public_repos = EXCLUDED.public_repos,
                followers = EXCLUDED.followers,
                total_stars = EXCLUDED.total_stars,
                total_forks = EXCLUDED.total_forks,
                velocity_score = EXCLUDED.velocity_score,
                primary_languages = EXCLUDED.primary_languages,
                top_repos = EXCLUDED.top_repos,
                last_fetched_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
        """)

        try:
            self.db.execute(
                insert_sql,
                {
                    **org,
                    "primary_languages": json.dumps(org.get("primary_languages", [])),
                    "top_repos": json.dumps(org.get("top_repos", [])),
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Failed to store org {org.get('login')}: {e}")
            self.db.rollback()

    async def _calculate_velocity_score(self, org: str, repos: List[Dict]) -> int:
        """
        Calculate developer velocity score (0-100).

        Components:
        - Commit frequency (30%): Recent activity
        - PR velocity (25%): Based on recent pushes
        - Issue resolution (20%): Open issues ratio
        - Contributor base (15%): Number of contributors
        - Release cadence (10%): Recent releases
        """
        if not repos:
            return 0

        # Filter active repos (pushed in last 6 months)
        six_months_ago = datetime.now() - timedelta(days=180)
        active_repos = [
            r
            for r in repos
            if r.get("pushed_at")
            and datetime.fromisoformat(r["pushed_at"].replace("Z", "+00:00")).replace(
                tzinfo=None
            )
            > six_months_ago
        ]

        # Commit frequency score (30%) - based on active repos ratio
        active_ratio = len(active_repos) / len(repos) if repos else 0
        commit_score = min(100, int(active_ratio * 100 * 1.2))

        # PR velocity score (25%) - based on recent push frequency
        recent_pushes = len(
            [
                r
                for r in repos
                if r.get("pushed_at")
                and datetime.fromisoformat(
                    r["pushed_at"].replace("Z", "+00:00")
                ).replace(tzinfo=None)
                > (datetime.now() - timedelta(days=30))
            ]
        )
        pr_score = min(100, int(recent_pushes * 5))

        # Issue resolution score (20%) - lower open issues is better
        total_issues = sum(r.get("open_issues_count", 0) for r in repos)
        total_stars = sum(r.get("stargazers_count", 0) for r in repos)
        # Normalize: fewer issues per star is better
        if total_stars > 0:
            issue_ratio = total_issues / total_stars
            issue_score = max(0, min(100, int(100 - issue_ratio * 500)))
        else:
            issue_score = 50

        # Contributor base score (15%) - estimate from repo count and stars
        contributor_estimate = min(len(repos) * 5, total_stars // 100)
        contributor_score = min(100, contributor_estimate)

        # Release cadence score (10%) - bonus for having releases
        release_score = 60  # Default moderate score

        # Weighted total
        velocity_score = int(
            commit_score * 0.30
            + pr_score * 0.25
            + issue_score * 0.20
            + contributor_score * 0.15
            + release_score * 0.10
        )

        return min(100, max(0, velocity_score))

    def get_organization(self, org: str) -> Optional[Dict]:
        """Get stored organization data."""
        query = text("""
            SELECT * FROM github_organizations WHERE login = :org
        """)
        result = self.db.execute(query, {"org": org})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "login": row["login"],
            "name": row["name"],
            "description": row["description"],
            "blog": row["blog"],
            "location": row["location"],
            "email": row["email"],
            "twitter_username": row["twitter_username"],
            "public_repos": row["public_repos"],
            "followers": row["followers"],
            "github_created_at": row["github_created_at"].isoformat()
            if row["github_created_at"]
            else None,
            "metrics": {
                "total_stars": row["total_stars"],
                "total_forks": row["total_forks"],
                "top_repos": row["top_repos"],
                "primary_languages": row["primary_languages"],
            },
            "velocity_score": row["velocity_score"],
            "last_fetched_at": row["last_fetched_at"].isoformat()
            if row["last_fetched_at"]
            else None,
        }

    def get_org_repos(
        self, org: str, limit: int = 50, offset: int = 0, sort_by: str = "stars"
    ) -> Dict[str, Any]:
        """Get repositories for an organization."""
        sort_column = {
            "stars": "stars DESC",
            "forks": "forks DESC",
            "updated": "pushed_at DESC NULLS LAST",
            "name": "name ASC",
        }.get(sort_by, "stars DESC")

        count_query = text("""
            SELECT COUNT(*) FROM github_repositories WHERE org_login = :org
        """)
        total = self.db.execute(count_query, {"org": org}).scalar()

        query = text(f"""
            SELECT * FROM github_repositories
            WHERE org_login = :org
            ORDER BY {sort_column}
            LIMIT :limit OFFSET :offset
        """)

        result = self.db.execute(query, {"org": org, "limit": limit, "offset": offset})
        repos = []
        for row in result.mappings():
            repos.append(
                {
                    "name": row["name"],
                    "full_name": row["full_name"],
                    "description": row["description"],
                    "language": row["language"],
                    "stars": row["stars"],
                    "forks": row["forks"],
                    "open_issues": row["open_issues"],
                    "is_fork": row["is_fork"],
                    "is_archived": row["is_archived"],
                    "topics": row["topics"],
                    "pushed_at": row["pushed_at"].isoformat()
                    if row["pushed_at"]
                    else None,
                    "github_created_at": row["github_created_at"].isoformat()
                    if row["github_created_at"]
                    else None,
                }
            )

        return {
            "org": org,
            "total": total,
            "limit": limit,
            "offset": offset,
            "repositories": repos,
        }

    async def get_org_activity(self, org: str, weeks: int = 12) -> Dict[str, Any]:
        """
        Get activity trends for an organization.

        Aggregates commit activity from top repos.
        """
        # Get top repos by stars
        query = text("""
            SELECT full_name FROM github_repositories
            WHERE org_login = :org
            ORDER BY stars DESC
            LIMIT 10
        """)
        result = self.db.execute(query, {"org": org})
        top_repos = [row[0] for row in result.fetchall()]

        weekly_activity = []
        total_commits = 0
        recent_commits = 0

        # Fetch commit activity for top repos
        for full_name in top_repos[:5]:  # Limit API calls
            owner, repo = full_name.split("/")
            activity = await self.client.get_commit_activity(owner, repo)
            if activity:
                for week_data in activity[-weeks:]:
                    total_commits += week_data.get("total", 0)
                if activity:
                    recent_commits += (
                        activity[-1].get("total", 0) if activity[-1] else 0
                    )

        # Generate weekly summary (simplified without full API data)
        today = date.today()
        for i in range(weeks):
            week_start = today - timedelta(weeks=weeks - i - 1, days=today.weekday())
            weekly_activity.append(
                {
                    "week": week_start.isoformat(),
                    "commits": total_commits // weeks if total_commits else 0,
                    "repos_active": len(top_repos),
                }
            )

        # Determine trends
        commit_trend = "stable"
        if len(weekly_activity) >= 4:
            first_half = sum(
                w["commits"] for w in weekly_activity[: len(weekly_activity) // 2]
            )
            second_half = sum(
                w["commits"] for w in weekly_activity[len(weekly_activity) // 2 :]
            )
            if second_half > first_half * 1.2:
                commit_trend = "increasing"
            elif second_half < first_half * 0.8:
                commit_trend = "decreasing"

        return {
            "org": org,
            "period": f"last_{weeks}_weeks",
            "weekly_activity": weekly_activity,
            "trends": {
                "commit_trend": commit_trend,
                "total_commits": total_commits,
            },
            "top_repos_analyzed": top_repos[:5],
        }

    def get_org_contributors(self, org: str, limit: int = 50) -> Dict[str, Any]:
        """Get top contributors for an organization."""
        query = text("""
            SELECT username, avatar_url, SUM(contributions) as total_contributions,
                   COUNT(DISTINCT repo_full_name) as repos_contributed
            FROM github_contributors
            WHERE org_login = :org
            GROUP BY username, avatar_url
            ORDER BY total_contributions DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, {"org": org, "limit": limit})
        contributors = []
        for row in result.mappings():
            contributors.append(
                {
                    "username": row["username"],
                    "avatar_url": row["avatar_url"],
                    "total_contributions": row["total_contributions"],
                    "repos_contributed": row["repos_contributed"],
                }
            )

        return {
            "org": org,
            "total_contributors": len(contributors),
            "contributors": contributors,
        }

    def get_velocity_breakdown(self, org: str) -> Optional[Dict[str, Any]]:
        """Get velocity score breakdown for an organization."""
        org_data = self.get_organization(org)
        if not org_data:
            return None

        velocity_score = org_data.get("velocity_score", 0)

        # Estimate component scores based on available data
        metrics = org_data.get("metrics", {})
        total_stars = metrics.get("total_stars", 0)
        total_forks = metrics.get("total_forks", 0)

        # These are estimates since we don't have detailed historical data
        return {
            "org": org,
            "velocity_score": velocity_score,
            "breakdown": {
                "commit_frequency": min(100, velocity_score + 10),
                "pr_velocity": velocity_score,
                "issue_resolution": max(0, velocity_score - 5),
                "contributor_growth": velocity_score,
                "release_cadence": 60,  # Default
            },
            "percentile": self._estimate_percentile(velocity_score),
            "comparison": self._get_comparison_text(velocity_score),
        }

    def _estimate_percentile(self, score: int) -> int:
        """Estimate percentile based on velocity score."""
        if score >= 80:
            return 95
        elif score >= 70:
            return 85
        elif score >= 60:
            return 70
        elif score >= 50:
            return 50
        elif score >= 40:
            return 30
        else:
            return 15

    def _get_comparison_text(self, score: int) -> str:
        """Get comparison text based on velocity score."""
        if score >= 80:
            return "Top 5% of tech organizations by developer velocity"
        elif score >= 70:
            return "Top 15% of tech organizations by developer velocity"
        elif score >= 60:
            return "Above average developer velocity"
        elif score >= 50:
            return "Average developer velocity"
        elif score >= 40:
            return "Below average developer velocity"
        else:
            return "Low developer velocity - may indicate maintenance mode"

    def get_repo_details(self, owner: str, repo: str) -> Optional[Dict]:
        """Get details for a specific repository."""
        query = text("""
            SELECT * FROM github_repositories WHERE full_name = :full_name
        """)
        result = self.db.execute(query, {"full_name": f"{owner}/{repo}"})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "name": row["name"],
            "full_name": row["full_name"],
            "description": row["description"],
            "homepage": row["homepage"],
            "language": row["language"],
            "languages": row["languages"],
            "stars": row["stars"],
            "forks": row["forks"],
            "watchers": row["watchers"],
            "open_issues": row["open_issues"],
            "size_kb": row["size_kb"],
            "default_branch": row["default_branch"],
            "is_fork": row["is_fork"],
            "is_archived": row["is_archived"],
            "topics": row["topics"],
            "license_name": row["license_name"],
            "github_created_at": row["github_created_at"].isoformat()
            if row["github_created_at"]
            else None,
            "pushed_at": row["pushed_at"].isoformat() if row["pushed_at"] else None,
            "last_fetched_at": row["last_fetched_at"].isoformat()
            if row["last_fetched_at"]
            else None,
        }

    def search_repos(self, query: str, limit: int = 20) -> Dict[str, Any]:
        """Search repositories in database."""
        search_query = text("""
            SELECT * FROM github_repositories
            WHERE name ILIKE :query OR description ILIKE :query
            ORDER BY stars DESC
            LIMIT :limit
        """)

        result = self.db.execute(search_query, {"query": f"%{query}%", "limit": limit})
        repos = []
        for row in result.mappings():
            repos.append(
                {
                    "name": row["name"],
                    "full_name": row["full_name"],
                    "description": row["description"],
                    "language": row["language"],
                    "stars": row["stars"],
                    "forks": row["forks"],
                }
            )

        return {"query": query, "count": len(repos), "repositories": repos}

    def get_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics."""
        stats_query = text("""
            SELECT
                COUNT(DISTINCT org_login) as total_orgs,
                COUNT(*) as total_repos,
                SUM(stars) as total_stars,
                SUM(forks) as total_forks,
                COUNT(DISTINCT language) as languages_count
            FROM github_repositories
        """)
        result = self.db.execute(stats_query).mappings().fetchone()

        # Top languages
        lang_query = text("""
            SELECT language, COUNT(*) as count, SUM(stars) as stars
            FROM github_repositories
            WHERE language IS NOT NULL
            GROUP BY language
            ORDER BY count DESC
            LIMIT 10
        """)
        lang_result = self.db.execute(lang_query)
        top_languages = [
            {"language": r[0], "count": r[1], "stars": r[2]}
            for r in lang_result.fetchall()
        ]

        return {
            "total_organizations": result["total_orgs"],
            "total_repositories": result["total_repos"],
            "total_stars": result["total_stars"],
            "total_forks": result["total_forks"],
            "languages_tracked": result["languages_count"],
            "top_languages": top_languages,
        }
