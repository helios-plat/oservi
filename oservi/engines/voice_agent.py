"""
oservi/engines/voice_agent.py — VoiceAgentEngine Skeleton.

Stateful engine skeleton for voice agent sessions.

五红线自检:
✅ 红线 1  反复出现: voice agent 是行业成熟形态 (LiveKit Agents, Vapi, Retell)
✅ 红线 2  机制/业务分离: 引擎固化音频管线循环; 业务靠注入
✅ 红线 3  注入点契约: 每注入点含 kind + cardinality
✅ 红线 4  无状态骨架: 持运行时状态, 不持业务状态
✅ 红线 5  不反向依赖: 骨架不硬编码 import oprim/oskill/omodul
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

# Graceful import: oservi may not be fully installed with all deps
try:
    from oservi.engines._base import (
        EngineSkeleton,
        Injection,
        InjectionKind,
        register_skeleton,
        ManifestValidationError,
    )
except ImportError:
    # Minimal stubs for standalone inspection
    class EngineSkeleton:
        injection_points: dict = {}
    class Injection:
        def __init__(self, *, kind=None, cardinality=None, description=None):
            self.kind = kind
            self.cardinality = cardinality
            self.description = description
    InjectionKind = str
    ManifestValidationError = Exception
    def register_skeleton(name, cls):
        pass


# ---------------------------------------------------------------------------
# Voice session state (runtime only)
# ---------------------------------------------------------------------------


@dataclass
class VoiceSessionState:
    """Runtime state for a voice agent session (not persisted)."""

    session_id: str = ""
    messages: list[dict] = field(default_factory=list)
    turns: int = 0
    total_input_audio_bytes: int = 0
    total_output_audio_bytes: int = 0
    start_ts: float = field(default_factory=time.time)
    events: list[dict] = field(default_factory=list)

    def record(self, **kwargs) -> None:
        self.events.append({"ts": time.time(), "turn": self.turns, **kwargs})


# ---------------------------------------------------------------------------
# VoiceAgentEngine skeleton
# ---------------------------------------------------------------------------


class VoiceAgentEngine(EngineSkeleton):
    """Voice agent engine — manages voice conversation sessions (on_demand).

    Orchestrates: audio in → VAD → STT → LLM → TTS → audio out.
    Supports turn management, interruption, and multi-turn dialogue.

    Injection points:
        stt_skill      — oskill.STT pipeline (speech_to_text)
        tts_skill      — oskill.TTS pipeline (text_to_speech)
        llm_caller     — oprim LLM call primitive
        vad_oprim      — oprim VAD operation (vad_frame)
        turn_detector  — oskill turn detection (TurnDetector)
        audio_io       — oskill audio pipeline (AudioPipeline)
        tools          — layer4 tool adapters (ToolSpec list)
    """

    injection_points = {
        "stt_skill": Injection(
            kind="oskill", cardinality="1",
            description="Speech-to-text pipeline: (audio, provider, **opts) -> TranscriptionResult"
        ),
        "tts_skill": Injection(
            kind="oskill", cardinality="1",
            description="Text-to-speech pipeline: (text, provider, **opts) -> bytes"
        ),
        "llm_caller": Injection(
            kind="oprim", cardinality="1",
            description="LLM call primitive: (messages, tools, max_tokens) -> dict"
        ),
        "vad_oprim": Injection(
            kind="oprim", cardinality="1",
            description="VAD frame-level operation: (frame, mode, threshold) -> VADResult"
        ),
        "turn_detector": Injection(
            kind="oskill", cardinality="0..1",
            description="Turn detection: TurnDetector instance or factory"
        ),
        "audio_io": Injection(
            kind="oskill", cardinality="0..1",
            description="Audio I/O pipeline: AudioPipeline instance or factory"
        ),
        "tools": Injection(
            kind="layer4", cardinality="0..n",
            description="Tool adapters for LLM function calling (ToolSpec list)"
        ),
    }

    def __init__(
        self,
        *,
        stt_skill,
        tts_skill,
        llm_caller,
        vad_oprim,
        turn_detector=None,
        audio_io=None,
        tools=None,
        trigger: dict,
        config: dict | None = None,
        name: str = "voice-agent",
    ):
        super().__init__()
        self.stt_skill = stt_skill
        self.tts_skill = tts_skill
        self.llm_caller = llm_caller
        self.vad_oprim = vad_oprim
        self.turn_detector = turn_detector
        self.audio_io = audio_io
        self.tools = tools or []
        self.trigger = trigger
        self.config = config or {}
        self.name = name

        # Runtime state (红线 4: 不在类定义层级)
        self._running = False
        self._current_session: VoiceSessionState | None = None

    def run(self) -> None:
        """on_demand 模式: 标记就绪, 调用方通过 session() 驱动."""
        self._running = True

    def stop(self) -> None:
        """优雅停止."""
        self._running = False

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "name": self.name,
            "current_session": self._current_session.session_id if self._current_session else None,
        }

    # ── Core session API ──────────────────────────────────────────────────

    async def session(
        self,
        audio_input: bytes,
        *,
        system_prompt: str = "You are a helpful voice assistant.",
        conversation_history: list[dict] | None = None,
        max_turns: int = 20,
        language: str = "en",
        stt_provider: str = "openai",
        tts_provider: str = "openai",
        tts_voice: str | None = None,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> dict:
        """Run a complete voice conversation session.

        Args:
            audio_input: Raw PCM audio bytes.
            system_prompt: LLM system prompt.
            conversation_history: Previous conversation turns.
            max_turns: Maximum conversation turns.
            language: Language code.
            stt_provider: STT provider name.
            tts_provider: TTS provider name.
            tts_voice: TTS voice ID.
            on_event: Callback(event_name, payload) for lifecycle events.

        Returns:
            Dict with result, transcript, audio_output, stats.
        """
        import uuid
        from veya.oprim.audio import split_into_frames
        from veya.oprim.types import AudioFrame
        from veya.oskill.turn_detection import TurnDetector, TurnHandlingConfig

        state = VoiceSessionState(
            session_id=str(uuid.uuid4())[:8],
        )
        self._current_session = state

        sample_rate = self.config.get("sample_rate", 16000)
        frame_duration_ms = self.config.get("frame_duration_ms", 20)

        # Initialize turn detector
        detector = self.turn_detector
        if detector is None:
            detector = TurnDetector(sample_rate=sample_rate)

        history = conversation_history or []
        output_audio = b""
        transcript: list[dict] = []

        if on_event:
            on_event("session_start", {"session_id": state.session_id})

        # Split audio into frames
        frames_data = split_into_frames(
            audio_input,
            frame_duration_ms=frame_duration_ms,
            sample_rate=sample_rate,
        )

        accumulated_speech: list[bytes] = []
        vad_state: dict = {}

        for i, frame_data in enumerate(frames_data):
            if not self._running:
                break

            frame = AudioFrame(
                data=frame_data,
                sample_rate=sample_rate,
                timestamp_ms=i * frame_duration_ms,
            )

            # VAD
            vad_result = self.vad_oprim(frame, _state=vad_state)

            if vad_result.is_speech:
                accumulated_speech.append(frame_data)

            # Turn detection
            decision = detector.process_frame(frame, vad_result)

            # Endpoint detected — process turn
            if decision.state.value in ("user_done", "interruption") and accumulated_speech:
                if on_event:
                    on_event("user_turn_end", {"reason": decision.reason})

                speech_bytes = b"".join(accumulated_speech)
                state.total_input_audio_bytes += len(speech_bytes)
                accumulated_speech = []

                # STT: transcribe
                if on_event:
                    on_event("transcribing", {})
                try:
                    stt_result = await self.stt_skill(
                        speech_bytes,
                        provider=stt_provider,
                        language=language,
                        sample_rate=sample_rate,
                    )
                except Exception as e:
                    state.record(event="stt_error", error=str(e))
                    continue

                if not stt_result.text.strip():
                    continue

                if on_event:
                    on_event("user_transcript", {"text": stt_result.text})

                transcript.append({"role": "user", "text": stt_result.text})
                state.messages.append({"role": "user", "content": stt_result.text})

                # LLM: generate response
                if on_event:
                    on_event("thinking", {})
                try:
                    llm_messages = [
                        {"role": "system", "content": system_prompt},
                    ]
                    llm_messages.extend(history[-10:])
                    llm_messages.append({"role": "user", "content": stt_result.text})

                    tools_schema = [
                        {"name": t.name, "description": t.description, "input_schema": t.input_schema}
                        for t in self.tools
                    ] if self.tools else None

                    llm_response = await self.llm_caller(
                        messages=llm_messages,
                        tools=tools_schema,
                        max_tokens=1024,
                    )
                    content_blocks = llm_response.get("content", [])
                    agent_text = ""
                    for block in content_blocks:
                        if block.get("type") == "text":
                            agent_text = block.get("text", "")
                            break
                    if not agent_text:
                        agent_text = content_blocks[0].get("text", "") if content_blocks else ""
                except Exception as e:
                    state.record(event="llm_error", error=str(e))
                    agent_text = "I'm sorry, I had trouble understanding that."

                if on_event:
                    on_event("agent_response", {"text": agent_text})

                transcript.append({"role": "assistant", "text": agent_text})
                state.messages.append({"role": "assistant", "content": agent_text})

                # TTS: synthesize
                if on_event:
                    on_event("speaking", {"text": agent_text})
                try:
                    agent_audio = await self.tts_skill(
                        agent_text,
                        provider=tts_provider,
                        voice=tts_voice,
                    )
                    output_audio += agent_audio
                    state.total_output_audio_bytes += len(agent_audio)
                except Exception as e:
                    state.record(event="tts_error", error=str(e))

                # Update history
                history.append({"role": "user", "content": stt_result.text})
                history.append({"role": "assistant", "content": agent_text})
                state.turns += 1

                detector.agent_stopped_speaking()

                if state.turns >= max_turns:
                    break

            # Interruption during agent speech
            elif decision.state.value == "interruption":
                if on_event:
                    on_event("interrupted", {})
                accumulated_speech = [frame_data]

        state.record(event="session_done", turns=state.turns)

        if on_event:
            on_event("session_done", {"turns": state.turns})

        return {
            "status": "completed",
            "transcript": transcript,
            "audio_output": output_audio,
            "turns": state.turns,
            "input_audio_bytes": state.total_input_audio_bytes,
            "output_audio_bytes": state.total_output_audio_bytes,
            "session_id": state.session_id,
            "events": state.events,
        }


# ── Register skeleton ────────────────────────────────────────────────────

register_skeleton("voice_agent", VoiceAgentEngine)
