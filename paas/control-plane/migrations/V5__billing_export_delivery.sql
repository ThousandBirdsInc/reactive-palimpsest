ALTER TABLE billing_exports
    ADD COLUMN delivery_ref TEXT,
    ADD COLUMN delivered_at TIMESTAMPTZ,
    ADD COLUMN error_message TEXT;
