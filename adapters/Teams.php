<?php
error_reporting(E_ALL & ~E_DEPRECATED);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Adapter started\n");

// === Helpers ===
function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}

// === Input Path ===
$inputPath = trim($argv[1] ?? '', " \t\n\r\0\x0B\"'");
if (!is_readable($inputPath)) {
    echo json_encode(["error" => "File not found or unreadable", "path" => $inputPath]);
    exit(1);
}

// === Load Project Ids for Teams assignment to projects ===
$projectIdPath = "C:\\Users\\steve\\OneDrive\\Documents\\Social Pinpoint\\Project\\SWC\\CM_SWC_Projects\\LookupProjectIdForTeams.csv";
$projectIds = [];

if (is_readable($projectIdPath)) {
    $projectLines = file($projectIdPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($projectLines as $line) {
        $parts = str_getcsv($line, ",", '"', "\\");
        if (count($parts) >= 2) {
            $id = trim($parts[0]);
            $name = trim($parts[1]);
            $projectIds[$name] = $id;
        }
    }
} else {
    fwrite(STDERR, "⚠️ Warning: Project ID lookup file not found\n");
}

// === Load CSV ===
$lines = file($inputPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
fwrite(STDERR, "📦 Php file read:\n");
fwrite(STDERR, print_r($lines, true));

if (count($lines) < 2) {
    echo json_encode(["error" => "CSV file is empty or malformed"]);
    exit(1);
}

// === Header Mapping ===
$headerMap = [
    "Source Id (Admin Only)" => "teamssourceid",
    "Name" => "name",
    "Description" => "description",
    "Projects" => "projects"
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM

$normalizedHeader = array_map(function ($col) use ($headerMap) {
    return $headerMap[$col] ?? $col;
}, $rawHeader);

// === Validate Columns ===
fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");
$expected = array_values($headerMap);
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    fwrite(STDERR, "⚠️ Warning: Missing expected columns: " . implode(", ", $missing) . "\n");
}

// === Build Records in Target Format ===
$records = [];
$skipped = 0;

foreach ($lines as $line) {
    $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
    if (count($fields) !== count($normalizedHeader)) {
        fwrite(STDERR, "⚠️ Skipping row with mismatched column count: " . json_encode($fields) . "\n");
        $skipped++;
        continue;
    }

    $row = array_combine($normalizedHeader, $fields);
    fwrite(STDERR, "🔍 Row: " . json_encode($row) . "\n");

    if (!$row || empty(trim($row["name"] ?? ""))) {
        fwrite(STDERR, "⚠️ Skipping row missing mandatory field 'name': " . json_encode($row) . "\n");
        $skipped++;
        continue;
    }

    // === Resolve project IDs ===
    $relateIds = [];
    $projectNames = explode(",", $row["projects"] ?? "");
    foreach ($projectNames as $projName) {
        $projName = trim($projName);
        if (isset($projectIds[$projName])) {
            $relateIds[] = $projectIds[$projName];
        }
    }

    $records[] = [
        "DataVersion" => 1,
        "ProjectOperations" => [
            "Relate" => $relateIds,
            "Unrelate" => [0]
        ],
        "Values" => [
            "description" => normalizeEmpty($row["description"] ?? ""),
            "name" => normalizeEmpty($row["name"] ?? "")
            // "teamssourceid" => normalizeEmpty($row["teamssourceid"] ?? "")
        ]
    ];
}

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "records" => $records
];
fwrite(STDERR, "⚠️ Skipped {$skipped} invalid rows\n");

$json = json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
if ($json === false) {
    fwrite(STDERR, "❌ JSON encoding failed: " . json_last_error_msg() . "\n");
    exit(1);
}

echo $json . "\n";
fwrite(STDERR, "✅ Adapter completed with {$output['recordCount']} records\n");