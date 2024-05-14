BEGIN TRANSACTION; 

CREATE TABLE IF NOT EXISTS account (
    customer_id INT NOT NULL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    address_1 TEXT,
    address_2 TEXT,
    city TEXT,
    "state" TEXT,
    zip_code TEXT,
    join_date DATE
);
CREATE INDEX IF NOT EXISTS account_name_idx ON account(first_name, last_name);
CREATE INDEX IF NOT EXISTS account_address_idx ON account("state", city, address_1, address_2);
CREATE INDEX IF NOT EXISTS account_zipcode_idx ON account(zip_code);
CREATE INDEX IF NOT EXISTS account_joindate_idx ON account(join_date);

CREATE TABLE IF NOT EXISTS product (
    product_id INT NOT NULL PRIMARY KEY,
    product_code TEXT,
    product_description TEXT
);
CREATE INDEX IF NOT EXISTS product_code_idx ON product(product_code);
CREATE INDEX IF NOT EXISTS product_desc_idx ON product(product_description);

CREATE TABLE IF NOT EXISTS "transaction" (
    transaction_id TEXT NOT NULL PRIMARY KEY,
    transaction_date DATE,
    quantity INT,
    product_id INT NOT NULL REFERENCES product (product_id),
    account_id INT NOT NULL REFERENCES account (customer_id)
);
CREATE INDEX IF NOT EXISTS transaction_date_idx ON "transaction"(transaction_date);
CREATE INDEX IF NOT EXISTS transaction_product_idx ON "transaction"(product_id);
CREATE INDEX IF NOT EXISTS transaction_account_idx ON "transaction"(account_id);

COMMIT;