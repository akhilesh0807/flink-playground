# Skill: Update Flink Learnings (`flink-update-learnings`)

## Purpose
Use this skill at the end of a session or after implementing Flink features to automatically extract new insights, debugging solutions (such as watermark behaviors, state backends, windowing patterns, or Java reflection fixes), and summarize them into the Flink expert agent file (`agent/flink-expert.md`).

## Instructions

1. **Review Session History:** Analyze the current chat session or any generated transcripts (e.g., `conversation_transcript.txt`) for key technical takeaways regarding Apache Flink, Java, Kafka integration, watermarking, or state management.
2. **Identify New Learnings:** Look for:
   - Newly discovered debugging steps or error resolutions (e.g., Java 17+ `InaccessibleObjectException`, Kafka timestamps defaulting to 1970).
   - Architectural decisions (e.g., choosing `KeyedProcessFunction` over sliding windows for count-based rolling averages).
   - Performance or state tuning insights.
3. **Update Agent File:** Read `agent/flink-expert.md` and append or refine the sections with the newly extracted learnings to keep the agent's knowledge base continuously up to date for future sessions.
