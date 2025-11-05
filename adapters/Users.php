<?php
error_reporting(E_ALL & ~E_DEPRECATED);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Adapter started\n");

$mode = strtolower($argv[2] ?? 'insert'); // 'insert' or 'update'

function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}
function normalizeEmail($email) {
    return strtolower(trim($email));
}

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

$records = [];

foreach ($lines as $line) {
    $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
    if (count($fields) !== count($normalizedHeader)) {
        fwrite(STDERR, "⚠️ Skipping row with mismatched column count: " . json_encode($fields) . "\n");
        continue;
    }

    $row = array_combine($normalizedHeader, $fields);

    // === Build Values block ===
    $values = [];

    foreach ($normalizedHeader as $key) {
        if ($key === "id") continue; // handled separately
        if ($mode === 'insert' || array_key_exists($key, $row)) {
            $values[$key] = normalizeEmpty($row[$key] ?? "");
        }
    }

    // === Add static fields ===
    $values["userStereotypes"] = [
        "assign" => [],
        "unassign" => []
    ];
    $values["userstatus"] = 0;
    $values["useLegacyLogin"] = false;
    $systemrole = array_filter(array_map('trim', explode(";", trim($row["systemvroles"] ?? ""))));
        
    // === Remove unneccessary fields ===
    unset($values["OAuth Identifier"]);
    unset($values["Descriptor"]);
    unset($values["Visibility"]);
    unset($values["Deleted"]);
    unset($values["useLegacyLogin"]);

    // === Build record ===
    $record = [
        "DataVersion" => 1,
        "Values" => $values
    ];

    if ($mode === 'update') {
        $record["id"] = trim($row["id"] ?? "");
        $record["ProjectOperations"] = [
            "Relate" => $systemrole,
            "Unrelate" => []
        ];
    }
    if ($mode === 'insert') {
        $record["SentOnboardingEmail"] = false;
    }

    $records[] = $record;
}

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "valueKey" => "Values",
    "projectOperationsKey" => "ProjectOperations",
    "dataVersionKey" => "DataVersion",
    "records" => $records
];


$json = json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
if ($json === false) {
    fwrite(STDERR, "❌ JSON encoding failed: " . json_last_error_msg() . "\n");
    exit(1);
}
echo $json . "\n";
fwrite(STDERR, "🔧 Mode: $mode\n");
fwrite(STDERR, "📦 Processed " . count($records) . " records\n");