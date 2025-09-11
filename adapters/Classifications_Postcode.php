<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Adapter started\n");

// === Input Path ===
$inputPath = trim($argv[1] ?? '', " \t\n\r\0\x0B\"'");
if (!is_readable($inputPath)) {
    echo json_encode(["error" => "File not found or unreadable", "path" => $inputPath]);
    exit(1);
}

// === Load CSV ===
$lines = file($inputPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
// Print debug to stderr
fwrite(STDERR, "📦 Php file read:\n");
print_r($lines, true);

if (count($lines) < 2) {
    echo json_encode(["error" => "CSV file is empty or malformed"]);
    exit(1);
}

// === Header Mapping ===
$headerMap = [
    "parent_id" => "parent_id",
    "name" => "name",
    "description" => "description",
    "header" => "header",
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM
$normalizedHeader = array_map(fn($col) => $headerMap[$col] ?? $col, $rawHeader);

// === Validate Columns ===
fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");
$expected = array_values($headerMap);
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    echo json_encode(["error" => "Missing columns", "missing" => $missing]);
    exit(1);
}

// === Build Records ===
$records = [];
foreach ($lines as $line) {
    $row = array_combine($normalizedHeader, array_map('trim', str_getcsv($line, ",", '"', "\\")));
    if (!$row) {
        fwrite(STDERR, "⚠️ Skipping malformed row: " . $line . "\n");
        continue;
    }
    if (!isset($row["name"]) || trim($row["name"]) === "") {
        fwrite(STDERR, "⚠️ Skipping row with empty name: " . json_encode($row) . "\n");
        continue;
    }

    // Normalize classificationType
    $classificationType = (strtoupper($row["header"]) === "TRUE") ? 1 : 2;

    // Build record
    $records[] = [
        "classificationType" => $classificationType,
        "dataVersion" => 0,
        "deleted" => false,
        "description" => $row["description"] ?: null,
        "name" => $row["name"],
        "parentId" => (int) $row["parent_id"]
    ];
}

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "records" => $records
];

echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
fwrite(STDERR, "✅ Adapter completed with {$output['recordCount']} records\n");