<?php
error_reporting(E_ALL & ~E_DEPRECATED);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Adapter started\n");

$mode = strtolower($argv[2] ?? 'insert'); // 'insert' or 'update'

// === Helpers ===
function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}
function normalizeEmail($email) {
    return strtolower(trim($email));
}

// === Editable whitelist (one-per-line for visibility) ===
$allowed = [
    'department',
    'email',
    'fax',
    'firstName',
    'lastName',
    'mobile',
    'notes',
    'organisation',
    'phone',
    'position',
    'useLegacyLogin',
    'usersourceid'
];

// === Input Path ===
$inputPath = trim($argv[1] ?? '', " \t\n\r\0\x0B\"'");
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
    "Source Id (Admin Only)" => "usersourceid",
    "First Name" => "firstName",
    "Last Name" => "lastName",
    "Position" => "position",
    "Department" => "department",
    "Organisation" => "organisation",
    "Phone" => "phone",
    "Mobile" => "mobile",
    "Fax" => "fax",
    "Email" => "email",
    "Login" => "login",
    // uncomment if you include System Role column in CSV and want to map it
    "System Role" => "systemrole",
    "Id" => "id" // used only in update mode
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM

$normalizedHeader = array_map(function ($col) use ($headerMap) {
    return $headerMap[$col] ?? $col;
}, $rawHeader);

fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");
fwrite(STDERR, "🔧 Mode: $mode\n");

// === Build Records in Target Format ===
$records = [];
foreach ($lines as $lineNumber => $line) {
    $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
    if (count($fields) !== count($normalizedHeader)) {
        fwrite(STDERR, "⚠️ Skipping row with mismatched column count (row " . ($lineNumber+2) . ")\n");
        continue;
    }

    $row = array_combine($normalizedHeader, $fields);

    // === Build normalized $values once from CSV ===
    $values = [];
    foreach ($normalizedHeader as $key) {
        if ($key === 'id') continue; // id is top-level for updates
        $values[$key] = normalizeEmpty($row[$key] ?? '');
    }

    // Ensure defaults present
    $values['useLegacyLogin'] = false;

    // === Parse roles from normalized key and ensure safe fallback ===
    $systemrole = array_filter(array_map('trim', explode(';', trim($row['systemrole'] ?? ''))));
    $systemrole = array_values(array_filter($systemrole, 'strlen'));
    if (empty($systemrole)) {
        $systemrole = ['StandardUser'];
    }

    // === Build final values using explicit whitelist (preserves order and shape) ===
    $finalValues = [];
    foreach ($allowed as $k) {
        if (array_key_exists($k, $values)) {
            $finalValues[$k] = $values[$k];
        } else {
            $finalValues[$k] = ($k === 'useLegacyLogin') ? false : '';
        }
    }

    // optional: log dropped keys for visibility
    $dropped = array_diff(array_keys($values), $allowed);
    if (!empty($dropped)) {
        fwrite(STDERR, "🧹 Dropped keys for row " . ($lineNumber+2) . ": " . implode(', ', $dropped) . "\n");
    }

    // === Build record ===
    $record = [
        'dataVersion' => 1,
        'values' => $finalValues
    ];

    if ($mode === 'insert') {
        $record['sendOnboardingEmail'] = false;
        $record['stereotypeOperations'] = [
            'Relate' => $systemrole,
            'Unrelate' => []
        ];
    } else { // update
        $id = normalizeEmpty($row['id'] ?? '');
        if ($id === '') {
            fwrite(STDERR, "⚠️ Skipping update row missing ID (row " . ($lineNumber+2) . ")\n");
            continue;
        }
        $record['id'] = $id;
        $record['projectOperations'] = [
            'Relate' => $systemrole,
            'Unrelate' => []
        ];
    }

    fwrite(STDERR, "📤 Built record (row " . ($lineNumber+2) . "): " . json_encode($record, JSON_UNESCAPED_SLASHES) . "\n");
    $records[] = $record;
}

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "valueKey" => "values",
    "dataVersionKey" => "dataVersion",
    "records" => $records
];

if ($mode === 'update') {
    $output["projectOperationsKey"] = "ProjectOperations";
}

// sample debug of first record (if any) for runner visibility
if (!empty($records)) {
    fwrite(STDERR, "🔬 Sample record JSON: " . json_encode($records[0], JSON_UNESCAPED_SLASHES) . "\n");
}

$json = json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
if ($json === false) {
    fwrite(STDERR, "❌ JSON encoding failed: " . json_last_error_msg() . "\n");
    exit(1);
}

echo $json . "\n";
fwrite(STDERR, "📦 Processed " . count($records) . " records\n");