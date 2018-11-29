USE employee_db;

BEGIN

DELETE FROM `employee_list` WHERE `id` = $id AND file_number <= $file_number;

$data

COMMIT;
