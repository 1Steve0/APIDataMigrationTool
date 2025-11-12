<?php
error_reporting(E_ALL & ~E_DEPRECATED);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 UserTeam Adapter started\n");

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
    $userId = normalizeEmpty($row["user"] ?? "");
    $teamId = normalizeEmpty($row["team"] ?? "");

    if ($userId === "" || $teamId === "") {
        fwrite(STDERR, "⚠️ Skipping row missing User or Team: " . json_encode($row) . "\n");
        $skipped++;
        continue;
    }

    try {
        $record = [
            "id" => $userId,
            "dataVersion" => 1,
            "teamOperations" => [
                "relate" => [intval($teamId)],
                "unrelate" => []
            ],
            "values" => new stdClass()
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