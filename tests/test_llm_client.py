"""
Unit tests for LLM Client.

Tests the unified async wrapper for OpenAI and Anthropic with mocked responses.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.agentic.llm_client import (
    LLMClient,
    LLMResponse,
    MODEL_PRICING,
    OPENAI_AVAILABLE,
    ANTHROPIC_AVAILABLE,
)


# ============================================================================
# LLMResponse Tests
# ============================================================================

class TestLLMResponse:
    """Tests for LLMResponse dataclass."""

    def test_response_creation(self):
        """Test basic response creation."""
        response = LLMResponse(
            content="Hello, world!",
            input_tokens=10,
            output_tokens=5,
            total_tokens=15,
            model="gpt-4o-mini",
            cost_usd=0.0001,
        )

        assert response.content == "Hello, world!"
        assert response.input_tokens == 10
        assert response.output_tokens == 5
        assert response.total_tokens == 15
        assert response.model == "gpt-4o-mini"
        assert response.cost_usd == 0.0001

    def test_parse_json_valid(self):
        """Test parsing valid JSON from response."""
        response = LLMResponse(
            content='{"companies": ["Apple", "Google"]}',
            input_tokens=10,
            output_tokens=20,
            total_tokens=30,
            model="gpt-4o-mini",
            cost_usd=0.0001,
        )

        result = response.parse_json()
        assert result == {"companies": ["Apple", "Google"]}

    def test_parse_json_with_markdown_code_block(self):
        """Test parsing JSON wrapped in markdown code blocks."""
        response = LLMResponse(
            content='```json\n{"name": "Test", "value": 42}\n```',
            input_tokens=10,
            output_tokens=20,
            total_tokens=30,
            model="gpt-4o-mini",
            cost_usd=0.0001,
        )

        result = response.parse_json()
        assert result == {"name": "Test", "value": 42}

    def test_parse_json_with_plain_code_block(self):
        """Test parsing JSON wrapped in plain code blocks."""
        response = LLMResponse(
            content='```\n{"items": [1, 2, 3]}\n```',
            input_tokens=10,
            output_tokens=20,
            total_tokens=30,
            model="gpt-4o-mini",
            cost_usd=0.0001,
        )

        result = response.parse_json()
        assert result == {"items": [1, 2, 3]}

    def test_parse_json_invalid(self):
        """Test parsing invalid JSON returns None."""
        response = LLMResponse(
            content='This is not JSON',
            input_tokens=10,
            output_tokens=20,
            total_tokens=30,
            model="gpt-4o-mini",
            cost_usd=0.0001,
        )

        result = response.parse_json()
        assert result is None

    def test_parse_json_nested_structure(self):
        """Test parsing complex nested JSON."""
        json_content = '''
        {
            "portfolio": [
                {"company": "Apple Inc", "shares": 1000},
                {"company": "Microsoft Corp", "shares": 500}
            ],
            "total_value": 150000.50
        }
        '''
        response = LLMResponse(
            content=json_content,
            input_tokens=50,
            output_tokens=100,
            total_tokens=150,
            model="claude-3-5-haiku-20241022",
            cost_usd=0.001,
        )

        result = response.parse_json()
        assert result is not None
        assert len(result["portfolio"]) == 2
        assert result["portfolio"][0]["company"] == "Apple Inc"
        assert result["total_value"] == 150000.50


# ============================================================================
# LLMClient Initialization Tests
# ============================================================================

class TestLLMClientInit:
    """Tests for LLMClient initialization."""

    def test_default_openai_init(self):
        """Test default initialization for OpenAI."""
        client = LLMClient(provider="openai", api_key="sk-test-key")

        assert client.provider == "openai"
        assert client.model == "gpt-4o-mini"
        assert client.max_tokens == 500
        assert client.temperature == 0.1
        assert client.max_retries == 3

    def test_default_anthropic_init(self):
        """Test default initialization for Anthropic."""
        client = LLMClient(provider="anthropic", api_key="sk-ant-test")

        assert client.provider == "anthropic"
        assert client.model == "claude-3-5-haiku-20241022"
        assert client.max_tokens == 500

    def test_custom_model(self):
        """Test initialization with custom model."""
        client = LLMClient(
            provider="openai",
            api_key="sk-test",
            model="gpt-4-turbo"
        )

        assert client.model == "gpt-4-turbo"

    def test_custom_parameters(self):
        """Test initialization with custom parameters."""
        client = LLMClient(
            provider="openai",
            api_key="sk-test",
            max_tokens=1000,
            temperature=0.7,
            max_retries=5,
            retry_delay=2.0,
        )

        assert client.max_tokens == 1000
        assert client.temperature == 0.7
        assert client.max_retries == 5
        assert client.retry_delay == 2.0

    def test_provider_case_insensitive(self):
        """Test that provider is case insensitive."""
        client1 = LLMClient(provider="OpenAI", api_key="sk-test")
        client2 = LLMClient(provider="ANTHROPIC", api_key="sk-test")

        assert client1.provider == "openai"
        assert client2.provider == "anthropic"


# ============================================================================
# LLMClient Availability Tests
# ============================================================================

class TestLLMClientAvailability:
    """Tests for is_available property."""

    @pytest.mark.skipif(not OPENAI_AVAILABLE, reason="OpenAI not installed")
    def test_openai_available_with_key(self):
        """Test OpenAI is available when package installed and key provided."""
        client = LLMClient(provider="openai", api_key="sk-test-key")
        assert client.is_available is True

    @pytest.mark.skipif(not ANTHROPIC_AVAILABLE, reason="Anthropic not installed")
    def test_anthropic_available_with_key(self):
        """Test Anthropic is available when package installed and key provided."""
        client = LLMClient(provider="anthropic", api_key="sk-ant-test")
        assert client.is_available is True

    def test_not_available_without_key(self):
        """Test client not available without API key."""
        client = LLMClient(provider="openai", api_key=None)
        assert client.is_available is False

    def test_not_available_empty_key(self):
        """Test client not available with empty API key."""
        client = LLMClient(provider="openai", api_key="")
        assert client.is_available is False


# ============================================================================
# Cost Calculation Tests
# ============================================================================

class TestCostCalculation:
    """Tests for cost calculation."""

    def test_calculate_cost_gpt4o_mini(self):
        """Test cost calculation for GPT-4o-mini."""
        client = LLMClient(provider="openai", api_key="sk-test")

        # 1000 input tokens, 500 output tokens
        cost = client._calculate_cost("gpt-4o-mini", 1000, 500)

        # Input: (1000/1M) * 0.15 = 0.00015
        # Output: (500/1M) * 0.60 = 0.0003
        # Total: 0.00045
        assert abs(cost - 0.00045) < 0.0001

    def test_calculate_cost_gpt4o(self):
        """Test cost calculation for GPT-4o."""
        client = LLMClient(provider="openai", api_key="sk-test")

        cost = client._calculate_cost("gpt-4o", 1000, 500)

        # Input: (1000/1M) * 2.50 = 0.0025
        # Output: (500/1M) * 10.00 = 0.005
        # Total: 0.0075
        assert abs(cost - 0.0075) < 0.0001

    def test_calculate_cost_claude_haiku(self):
        """Test cost calculation for Claude Haiku."""
        client = LLMClient(provider="anthropic", api_key="sk-ant-test")

        cost = client._calculate_cost("claude-3-5-haiku-20241022", 1000, 500)

        # Input: (1000/1M) * 0.80 = 0.0008
        # Output: (500/1M) * 4.00 = 0.002
        # Total: 0.0028
        assert abs(cost - 0.0028) < 0.0001

    def test_calculate_cost_unknown_model(self):
        """Test cost calculation for unknown model returns 0."""
        client = LLMClient(provider="openai", api_key="sk-test")

        cost = client._calculate_cost("unknown-model", 1000, 500)
        assert cost == 0.0

    def test_model_pricing_completeness(self):
        """Test that common models have pricing defined."""
        expected_models = [
            "gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo",
            "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022",
        ]

        for model in expected_models:
            assert model in MODEL_PRICING
            assert "input" in MODEL_PRICING[model]
            assert "output" in MODEL_PRICING[model]


# ============================================================================
# OpenAI Completion Tests (Mocked)
# ============================================================================

class TestOpenAICompletion:
    """Tests for OpenAI completion with mocked responses."""

    @pytest.mark.asyncio
    async def test_openai_complete_success(self):
        """Test successful OpenAI completion."""
        # Create mock response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test response"
        mock_response.usage = MagicMock()
        mock_response.usage.prompt_tokens = 50
        mock_response.usage.completion_tokens = 25

        # Create mock client
        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            with patch("app.agentic.llm_client.AsyncOpenAI", return_value=mock_openai_client):
                client = LLMClient(provider="openai", api_key="sk-test")
                client._client = mock_openai_client

                response = await client.complete("Hello")

                assert response.content == "Test response"
                assert response.input_tokens == 50
                assert response.output_tokens == 25
                assert response.total_tokens == 75

    @pytest.mark.asyncio
    async def test_openai_complete_with_system_prompt(self):
        """Test OpenAI completion with system prompt."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Extracted data"
        mock_response.usage = MagicMock()
        mock_response.usage.prompt_tokens = 100
        mock_response.usage.completion_tokens = 50

        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(provider="openai", api_key="sk-test")
            client._client = mock_openai_client

            response = await client.complete(
                "Extract companies",
                system_prompt="You are a data extraction assistant."
            )

            # Verify system prompt was passed
            call_args = mock_openai_client.chat.completions.create.call_args
            messages = call_args.kwargs["messages"]
            assert len(messages) == 2
            assert messages[0]["role"] == "system"
            assert messages[1]["role"] == "user"

    @pytest.mark.asyncio
    async def test_openai_complete_json_mode(self):
        """Test OpenAI completion with JSON mode."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '{"result": "ok"}'
        mock_response.usage = MagicMock()
        mock_response.usage.prompt_tokens = 30
        mock_response.usage.completion_tokens = 10

        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(provider="openai", api_key="sk-test")
            client._client = mock_openai_client

            response = await client.complete("Get data", json_mode=True)

            # Verify response_format was passed
            call_args = mock_openai_client.chat.completions.create.call_args
            assert call_args.kwargs["response_format"] == {"type": "json_object"}

            # Verify JSON parsing works
            data = response.parse_json()
            assert data == {"result": "ok"}


# ============================================================================
# Anthropic Completion Tests (Mocked)
# ============================================================================

class TestAnthropicCompletion:
    """Tests for Anthropic completion with mocked responses."""

    @pytest.mark.asyncio
    async def test_anthropic_complete_success(self):
        """Test successful Anthropic completion."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = "Claude response"
        mock_response.usage = MagicMock()
        mock_response.usage.input_tokens = 40
        mock_response.usage.output_tokens = 20

        mock_anthropic_client = AsyncMock()
        mock_anthropic_client.messages.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.ANTHROPIC_AVAILABLE", True):
            client = LLMClient(provider="anthropic", api_key="sk-ant-test")
            client._client = mock_anthropic_client

            response = await client.complete("Hello Claude")

            assert response.content == "Claude response"
            assert response.input_tokens == 40
            assert response.output_tokens == 20
            assert response.total_tokens == 60

    @pytest.mark.asyncio
    async def test_anthropic_complete_with_system_prompt(self):
        """Test Anthropic completion with system prompt."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = "System aware response"
        mock_response.usage = MagicMock()
        mock_response.usage.input_tokens = 80
        mock_response.usage.output_tokens = 40

        mock_anthropic_client = AsyncMock()
        mock_anthropic_client.messages.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.ANTHROPIC_AVAILABLE", True):
            client = LLMClient(provider="anthropic", api_key="sk-ant-test")
            client._client = mock_anthropic_client

            await client.complete(
                "Analyze data",
                system_prompt="You are an analyst."
            )

            # Verify system prompt was passed
            call_args = mock_anthropic_client.messages.create.call_args
            assert call_args.kwargs["system"] == "You are an analyst."


# ============================================================================
# Retry Logic Tests
# ============================================================================

class TestRetryLogic:
    """Tests for retry behavior."""

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Test that client retries on failure."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Success after retry"
        mock_response.usage = MagicMock()
        mock_response.usage.prompt_tokens = 10
        mock_response.usage.completion_tokens = 5

        mock_openai_client = AsyncMock()
        # Fail twice, then succeed
        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[
                Exception("API Error 1"),
                Exception("API Error 2"),
                mock_response,
            ]
        )

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(
                provider="openai",
                api_key="sk-test",
                max_retries=3,
                retry_delay=0.01,  # Fast retries for testing
            )
            client._client = mock_openai_client

            response = await client.complete("Test")

            assert response.content == "Success after retry"
            assert mock_openai_client.chat.completions.create.call_count == 3

    @pytest.mark.asyncio
    async def test_exhausted_retries(self):
        """Test that exception is raised after all retries exhausted."""
        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=Exception("Persistent failure")
        )

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(
                provider="openai",
                api_key="sk-test",
                max_retries=2,
                retry_delay=0.01,
            )
            client._client = mock_openai_client

            with pytest.raises(Exception) as exc_info:
                await client.complete("Test")

            assert "Persistent failure" in str(exc_info.value)
            assert mock_openai_client.chat.completions.create.call_count == 2

    @pytest.mark.asyncio
    async def test_no_retry_on_success(self):
        """Test that successful call doesn't trigger retries."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "First try success"
        mock_response.usage = MagicMock()
        mock_response.usage.prompt_tokens = 10
        mock_response.usage.completion_tokens = 5

        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(provider="openai", api_key="sk-test", max_retries=3)
            client._client = mock_openai_client

            await client.complete("Test")

            assert mock_openai_client.chat.completions.create.call_count == 1


# ============================================================================
# Token Tracking Tests
# ============================================================================

class TestTokenTracking:
    """Tests for token and cost tracking."""

    @pytest.mark.asyncio
    async def test_token_accumulation(self):
        """Test that tokens accumulate across calls."""
        mock_openai_client = AsyncMock()

        def create_response(prompt_tokens, completion_tokens):
            resp = MagicMock()
            resp.choices = [MagicMock()]
            resp.choices[0].message.content = "Response"
            resp.usage = MagicMock()
            resp.usage.prompt_tokens = prompt_tokens
            resp.usage.completion_tokens = completion_tokens
            return resp

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[
                create_response(100, 50),
                create_response(200, 100),
            ]
        )

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(provider="openai", api_key="sk-test")
            client._client = mock_openai_client

            await client.complete("First call")
            assert client.total_tokens_used == 150

            await client.complete("Second call")
            assert client.total_tokens_used == 450  # 150 + 300

    @pytest.mark.asyncio
    async def test_cost_accumulation(self):
        """Test that costs accumulate across calls."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Response"
        mock_response.usage = MagicMock()
        mock_response.usage.prompt_tokens = 1000
        mock_response.usage.completion_tokens = 500

        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(provider="openai", api_key="sk-test", model="gpt-4o-mini")
            client._client = mock_openai_client

            await client.complete("Test")

            # Expected cost: (1000/1M)*0.15 + (500/1M)*0.60 = 0.00045
            assert client.total_cost_usd > 0
            assert abs(client.total_cost_usd - 0.00045) < 0.0001

    def test_reset_stats(self):
        """Test that reset_stats clears tracking."""
        client = LLMClient(provider="openai", api_key="sk-test")
        client._total_tokens_used = 1000
        client._total_cost_usd = 0.05

        client.reset_stats()

        assert client.total_tokens_used == 0
        assert client.total_cost_usd == 0.0


# ============================================================================
# Edge Cases Tests
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_complete_without_available_provider(self):
        """Test that complete raises error when provider not available."""
        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", False):
            client = LLMClient(provider="openai", api_key="sk-test")

            with pytest.raises(ValueError) as exc_info:
                await client.complete("Test")

            assert "not available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_empty_response_content(self):
        """Test handling of empty response content."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = None
        mock_response.usage = MagicMock()
        mock_response.usage.prompt_tokens = 10
        mock_response.usage.completion_tokens = 0

        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(provider="openai", api_key="sk-test")
            client._client = mock_openai_client

            response = await client.complete("Test")

            assert response.content == ""

    @pytest.mark.asyncio
    async def test_missing_usage_data(self):
        """Test handling of response without usage data."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Response"
        mock_response.usage = None

        mock_openai_client = AsyncMock()
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("app.agentic.llm_client.OPENAI_AVAILABLE", True):
            client = LLMClient(provider="openai", api_key="sk-test")
            client._client = mock_openai_client

            response = await client.complete("Test")

            assert response.input_tokens == 0
            assert response.output_tokens == 0
            assert response.total_tokens == 0

    def test_parse_json_with_whitespace(self):
        """Test parsing JSON with extra whitespace."""
        response = LLMResponse(
            content='  \n  {"key": "value"}  \n  ',
            input_tokens=10,
            output_tokens=10,
            total_tokens=20,
            model="gpt-4o-mini",
            cost_usd=0.0001,
        )

        result = response.parse_json()
        assert result == {"key": "value"}

    def test_parse_json_with_multiline_code_block(self):
        """Test parsing JSON in multiline code block."""
        content = '''```json
{
    "companies": [
        "Apple",
        "Google",
        "Microsoft"
    ],
    "count": 3
}
```'''
        response = LLMResponse(
            content=content,
            input_tokens=10,
            output_tokens=30,
            total_tokens=40,
            model="gpt-4o-mini",
            cost_usd=0.0001,
        )

        result = response.parse_json()
        assert result is not None
        assert result["count"] == 3
        assert len(result["companies"]) == 3
