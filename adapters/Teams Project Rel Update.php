<!-- adapters/teams Project Rel Update.php -->

<?php
error_reporting(E_ALL & ~E_DEPRECATED);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 TeamProject Adapter started\n");

// === Helpers ===
function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}

// === Input Path and Mode ===
$inputPath = trim($argv[1] ?? '', " \t\n\r\0\x0B\"'");
$migrationType = strtolower(trim($argv[2] ?? 'update'));
fwrite(STDERR, "🔧 Migration mode: $migrationType\n");

if (!is_readable($inputPath)) {
    echo json_encode(["error" => "File not found or unreadable", "path" => $inputPath]);
    exit(1);
}

// === Load CSV ===
$lines = file($inputPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
if (count($lines) < 2) {
    echo json_encode(["error" => "CSV file is empty or malformed"]);
    exit(1);
}

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM
fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");

$normalizedHeader = array_map('strtolower', $rawHeader);
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
    $teamId = normalizeEmpty($row["team"] ?? "");
    $projectId = normalizeEmpty($row["project"] ?? "");

    if ($teamId === "" || $projectId === "") {
        fwrite(STDERR, "⚠️ Skipping row missing Team or Project: " . json_encode($row) . "\n");
        $skipped++;
        continue;
    }

    try {
        $record = [
            "id" => $teamId,
            "dataVersion" => 1,
            "projectOperations" => [
                "relate" => [intval($projectId)],
                "unrelate" => []
            ],
            "values" => new stdClass(), // empty object
            "meta" => [
                "id" => $teamId,  // 👈 Required for PATCH
                "rowIndex" => count($records) + 2, // +2 accounts for 0-based index + header row
                "source" => $row
                ]
        ];

        $records[] = $record;

        if (getenv('ADAPTER_DEBUG') === '1') {
            fwrite(STDERR, "📦 Packet debug: " . json_encode($record, JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT) . "\n");
        }
    } catch (Exception $e) {
        fwrite(STDERR, "❌ Exception while building record: " . $e->getMessage() . "\n");
        $skipped++;
        continue;
    }
}
$auditRows = [];
foreach ($records as $record) {
    $source = $record["meta"]["source"];
    $auditRows[] = [
        "rowIndex" => $record["meta"]["rowIndex"],
        "team" => $source["team"] ?? "",
        "project" => $source["project"] ?? ""
    ];
}
// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "adapter_key"=> "teams_projects_relationship",
    "records" => $records,
     "auditRows" => $auditRows
];

fwrite(STDERR, "⚠️ Skipped {$skipped} invalid rows\n");
$json = json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
if ($json === false) {
    fwrite(STDERR, "❌ JSON encoding failed: " . json_last_error_msg() . "\n");
    exit(1);
}
echo $json . "\n";
