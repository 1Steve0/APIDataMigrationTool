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
// fwrite(STDERR, "📦 Php file read:\n");
// fwrite(STDERR, print_r($lines, true));

if (count($lines) < 2) {
    echo json_encode(["error" => "CSV file is empty or malformed"]);
    exit(1);
}

// === Remove Users with an email which is in CM - We dont want to migrate duplicates ===
// === Load CM email list ===
$cmEmailPath = "C:\\Users\\steve\\OneDrive\\Documents\\Social Pinpoint\\Project\\SWC\\CM_SWC_Users\\CM_SWC_Users.csv";
$cmEmails = [];

if (is_readable($cmEmailPath)) {
    $cmLines = file($cmEmailPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    $cmHeader = array_map('trim', str_getcsv(array_shift($cmLines), ",", '"', "\\"));
    $emailIndex = array_search("Email", $cmHeader);

    if ($emailIndex !== false) {
        foreach ($cmLines as $cmLine) {
            $cmFields = str_getcsv($cmLine, ",", '"', "\\");
            $email = strtolower(trim($cmFields[$emailIndex] ?? ""));
            if ($email !== "") {
                $cmEmails[$email] = true;
            }
        }
        fwrite(STDERR, "📬 Loaded " . count($cmEmails) . " known CM emails\n");
    } else {
        fwrite(STDERR, "⚠️ CM email file missing 'Email' column\n");
    }
} else {
    fwrite(STDERR, "⚠️ CM email file not found or unreadable: $cmEmailPath\n");
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
    // "OAuth Identifier" => "oauthIdentifier",
    "Login" => "login"
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
$skipped = 0;

foreach ($lines as $line) {
    $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
    if (count($fields) !== count($normalizedHeader)) {
        fwrite(STDERR, "⚠️ Skipping row with mismatched column count: " . json_encode($fields) . "\n");
        $skipped++;
        continue;
    }
    // Only import new emails not in cm
    $emailCheck = strtolower(trim($fields[array_search("email", $normalizedHeader)] ?? ""));
    if (isset($cmEmails[$emailCheck])) {
        fwrite(STDERR, "🚫 Skipping duplicate email already in CM: $emailCheck\n");
        $skipped++;
        continue;
    }

    $row = array_combine($normalizedHeader, $fields);
    // fwrite(STDERR, "🔍 Row: " . json_encode($row) . "\n");

    if (
        !$row ||
        empty(trim($row["firstName"] ?? "")) ||
        empty(trim($row["email"] ?? ""))
    ) {
        fwrite(STDERR, "⚠️ Skipping row missing mandatory fields (firstName/email): " . json_encode($row) . "\n");
        $skipped++;
        continue;
    }

    try {
        $records[] = [
            "dataVersion" => 1,
            "stereotypeOperations" => [
                "Relate" => ["StandardUser"],
                "Unrelate" => []
            ],
            "sendOnboardingEmail" => false,
            "values" => [
                "department"     => normalizeEmpty($row["department"] ?? ""),
                "email"          => normalizeEmpty($row["email"] ?? ""),
                "fax"            => normalizeEmpty($row["fax"] ?? ""),
                "firstName"      => normalizeEmpty($row["firstName"] ?? ""),
                "lastName"       => normalizeEmpty($row["lastName"] ?? ""),
                "mobile"         => normalizeEmpty($row["mobile"] ?? ""),
                "notes"          => normalizeEmpty($row["notes"] ?? ""),
                "organisation"   => normalizeEmpty($row["organisation"] ?? ""),
                "phone"          => normalizeEmpty($row["phone"] ?? ""),
                "position"       => normalizeEmpty($row["position"] ?? ""),
                "useLegacyLogin" => false,
                "usersourceid"   => normalizeEmpty($row["usersourceid"] ?? "")
            ]
        ];
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
//fwrite(STDERR, "✅ Adapter completed with {$output['recordCount']} records\n");
