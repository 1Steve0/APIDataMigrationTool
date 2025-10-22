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
    "Name" => "name",
    "TimeZone" => "timeZone",
    "Group" => "projectGroup",
    "Notes" => "notes",
    "Address" => ["address", "address"],
    "Suburb" => ["address", "suburb"],
    "State" => ["address", "state"],
    "Post Code" => ["address", "postCode"],
    "Country" => ["address", "country"],
    "Location" => ["address", "location"],
    "Auto Geocode" => ["address", "autoGeocode"],
    "Legacy ID" => "legacyidprojects",
    "Project Email" => "projectemailaddress"
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM

$normalizedHeader = array_map(function ($col) use ($headerMap) {
    $mapped = $headerMap[$col] ?? $col;
    return is_array($mapped) ? implode('.', $mapped) : $mapped;
}, $rawHeader);

// === Validate Columns ===
fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");
$expected = array_map(function ($v) {
    return is_array($v) ? implode('.', $v) : $v;
}, array_values($headerMap));
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    fwrite(STDERR, "⚠️ Warning: Missing expected columns: " . implode(", ", $missing) . "\n");
    // Optionally: remove missing keys from $expected if used later
}

// === Build Records in Target Format ===
$records = [];
$timestamp = date("Y-m-d\TH:i:s"); // e.g., "2025-10-22T00:22:30"

foreach ($lines as $line) {
    $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
    if (count($fields) !== count($normalizedHeader)) {
        fwrite(STDERR, "⚠️ Skipping row with mismatched column count: " . json_encode($fields) . "\n");
        continue;
    }

    $row = array_combine($normalizedHeader, $fields);
    fwrite(STDERR, "🔍 Row: " . json_encode($row) . "\n");

    if (!$row || !isset($row["name"]) || trim($row["name"]) === "") {
        fwrite(STDERR, "⚠️ Skipping invalid row: " . json_encode($row) . "\n");
        continue;
    }

    $groupValues = isset($row["projectGroup"]) && is_string($row["projectGroup"])
        ? array_filter(array_map('intval', explode(',', $row["projectGroup"])))
        : [];

    // Build filtered payload
    $values = [
        "address.address"     => normalizeEmpty($row["address.address"] ?? ""),
        "address.suburb"      => normalizeEmpty($row["address.suburb"] ?? ""),
        "address.state"       => normalizeEmpty($row["address.state"] ?? ""),
        "address.postCode"    => normalizeEmpty($row["address.postCode"] ?? ""),
        "address.country"     => normalizeEmpty($row["address.country"] ?? ""),
        "address.location"    => normalizeEmpty($row["address.location"] ?? ""),
        "address.autoGeocode" => filter_var($row["address.autoGeocode"] ?? false, FILTER_VALIDATE_BOOLEAN),
        "dateEnd"                => $timestamp,
        "dateStart"              => $timestamp,
        "name"                   => normalizeEmpty($row["name"]),
        "notes"                  => normalizeEmpty($row["notes"] ?? ""),
        "projectGroup" => [
            "assign" => $groupValues,
            "unassign" => []
        ],
        "timeZone"               => normalizeEmpty($row["timeZone"] ?? "")
    ];

    $records[] = [
        "dataVersion" => 1,
        "Values" => $values
    ];
}

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "records" => $records
];

echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";
fwrite(STDERR, "✅ Adapter completed with {$output['recordCount']} records\n");

