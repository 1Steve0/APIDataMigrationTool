<?php
/**
 * Adapter: Stakeholder Classifications
 * Description: Bulk importer for Classification Management. Eg Event Type, Stakeholder Groups, Topics, Tags. Reads CSV, validates columns, transforms data for API
 * Author: Steven Hender
 */

header('Content-Type: application/json');

// === CONFIG ===
$expected_columns = ['Name', 'Email', 'Role'





];  // Customize per entity
$input_file = isset($argv[1]) ? $argv[1] : null;

if (!$input_file || !file_exists($input_file)) {
    echo json_encode(['error' => 'Input file not found']);
    exit(1);
}

// === LOAD CSV ===
$data = array_map('str_getcsv', file($input_file));
$headers = array_map('trim', $data[0]);

// === VALIDATE HEADERS ===
$missing = array_diff($expected_columns, $headers);
if (!empty($missing)) {
    echo json_encode(['error' => 'Missing columns: ' . implode(', ', $missing)]);
    exit(1);
}

// === TRANSFORM DATA ===
$records = [];
for ($i = 1; $i < count($data); $i++) {
    $row = array_combine($headers, $data[$i]);

    // === Manipulate Data Here ===
    $record = [
        'name' => $row['Name'],
        'email' => strtolower($row['Email']),
        'role' => ucfirst($row['Role']),
        // Add more transformations as needed
    ];

    $records[] = $record;
}

// === RETURN JSON ===
echo json_encode([
    'status' => 'success',
    'record_count' => count($records),
    'records' => $records
]);
exit(0);
?>