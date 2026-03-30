-- 1) Number of matches without events.
SELECT COUNT(*) AS matches_without_events
FROM "[vapi].MATCHES"
WHERE "EVENT_ID" IS NULL;

-- 2) Number of events without standings.
SELECT COUNT(*) AS events_without_standings
FROM "[vapi].EVENTS" e
WHERE NOT EXISTS (
    SELECT 1
    FROM "[vapi].EVENT_STANDINGS" s
    WHERE s."EVENT_ID" = e."EVENT_ID"
);
