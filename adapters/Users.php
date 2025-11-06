<?php
error_reporting(E_ALL & ~E_DEPRECATED);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Adapter started\n");

// === Helpers ===
function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}

// === Input Path and Mode ===
$inputPath = trim($argv[1] ?? '', " \t\n\r\0\x0B\"'");
$migrationType = strtolower(trim($argv[2] ?? 'insert'));
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

// === Header Mapping ===
$headerMap = [
    "source id (admin only)" => "usersourceid",
    "first name"             => "firstName",
    "last name"              => "lastName",
    "position"               => "position",
    "department"             => "department",
    "organisation"           => "organisation",
    "organisation name"      => "organisation",
    "phone"                  => "phone",
    "mobile"                 => "mobile",
    "fax"                    => "fax",
    "email"                  => "email",
    "login"                  => "login"
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM
$lcHeaderMap = array_change_key_case($headerMap, CASE_LOWER);

$normalizedHeader = [];
$ignoredColumns = [];
foreach ($rawHeader as $idx => $col) {
    $lk = strtolower(trim($col));
    if ($lk === 'id') {
        $normalizedHeader[] = '__ID__';
        $ignoredColumns[$idx] = true;
        continue;
    }
    $normalizedHeader[] = $lcHeaderMap[$lk] ?? trim($col);
}

// === Validate Columns ===
fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");
$expected = array_values(array_unique($lcHeaderMap));
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    fwrite(STDERR, "⚠️ Warning: Missing expected columns: " . implode(", ", $missing) . "\n");
}

// === Build Records ===
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

    if ($migrationType === "insert" && (
        empty(trim($row["firstName"] ?? "")) || empty(trim($row["email"] ?? ""))
    )) {
        fwrite(STDERR, "⚠️ Skipping row missing mandatory fields (firstName/email): " . json_encode($row) . "\n");
        $skipped++;
        continue;
    }

    try {
        $record = [
            "dataVersion" => 1,
            "sendOnboardingEmail" => false,
            "values" => [],
            "stereotypeOperations" => [
                "Relate" => ["StandardUser"],
                "Unrelate" => []
            ]
        ];

        if ($migrationType === "update" && !empty($row["__ID__"])) {
            $record["id"] = $row["__ID__"];
        }

        foreach ($normalizedHeader as $idx => $key) {
            if ($key === '__ID__') continue;
            if (in_array($key, $expected, true) && array_key_exists($key, $row)) {
                $record["values"][$key] = normalizeEmpty($row[$key]);
            }
        }
        if ($migrationType === "update"){
            $record["values"]["useLegacyLogin"] = $record["values"]["useLegacyLogin"] ?? false;
            $record["values"]["login"] = $record["values"]["login"] ?? "";
        }

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
