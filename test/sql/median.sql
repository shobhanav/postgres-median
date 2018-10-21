CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- #AV (Negative test case) Even number of Text values
INSERT INTO textvals VALUES
       ('mel',10);
SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Test table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 10) as T(i);

SELECT median(val) FROM timestampvals;

-- #AV BigInt values
CREATE TABLE bigintvals(val bigint, color text);
-- BigInts with odd number of values
INSERT INTO bigintvals VALUES
       (10000056798, 'a'),
       (20056077823, 'c'),
       (80123566778, 'b'),
       (72345678889, 'c'),
       (20056077825, 'd'),
       (-3234455667, 'd'),
       (20056077818, 'e');
SELECT * FROM bigintvals ORDER BY val;

SELECT median(val) FROM bigintvals;

-- #AV BigInts with even number of values
INSERT INTO bigintvals VALUES
       (20056077818, 'f');
SELECT * FROM bigintvals ORDER BY val;

SELECT median(val) FROM bigintvals;
