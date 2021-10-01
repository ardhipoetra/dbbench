-- Create table
\benchmark once \name init
CREATE TABLE dbbench_simple (id INT PRIMARY KEY, balance DECIMAL);

-- How long takes it in a single transaction?
\benchmark loop \name insertx
BEGIN TRANSACTION;
{{- range $idx, $e := call .Iterate 500 }}
INSERT INTO adbbench_simple (id, balance) VALUES({{printf "%d%03d" $.Iter $idx}}, {{$e}});
{{- end}}
COMMIT;

-- Delete table
\benchmark once \name clean
DROP TABLE dbbench_simple;