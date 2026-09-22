-- Separate database for `pytest`, which drops and recreates all tables
-- around every test. Keeping it apart from the app's own `agent_relay`
-- database means running the suite never resets a dev/compose instance's data.
CREATE DATABASE agent_relay_test;
