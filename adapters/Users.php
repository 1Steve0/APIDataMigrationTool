<?php
error_reporting(E_ALL & ~E_DEPRECATED);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Adapter started\n");

// === Helpers ===
function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}
function log_audit($fp,$idx,$first,$last,$email,$msg,$result){
    fputcsv($fp,[$idx,$first??"", $last??"", $email??"", $msg, $result]);
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

// === Audit setup ===
$adapterName = "users";
$repoRoot = dirname(__DIR__);
$reportDir = $repoRoot . "/auditreports";
if (!is_dir($reportDir)) { mkdir($reportDir, 0777, true); }
$auditFile = $reportDir . "/migration_log_" . $adapterName . "_" . date("Ymd_His") . ".csv";
$fp = fopen($auditFile, "w");
fputcsv($fp, ["rowIndex","firstName","lastName","email","message","result"]);

// === Header Mapping ===
$headerMap = [
    "id"                   => "id",
    "source id (admin only)" => "userssourceid",
    "first name"           => "firstName",
    "last name"            => "lastName",
    "position"             => "position",
    "department"           => "department",
    "organisation"         => "organisation",
    "organisation name"    => "organisation",
    "phone"                => "phone",
    "mobile"               => "mobile",
    "email"                => "email",
    "sendonboardingemail"  => "sendonboardingemail",
    "system role"          => "System Role"
];

$roleMap = [
    "StandardUser"            => "StandardUser",
    "EnterpriseAdministrator" => "EnterpriseAdministrator",
    "Admin"                   => "EnterpriseAdministrator",
    "User"                    => "StandardUser"
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]);
$lcHeaderMap = array_change_key_case($headerMap, CASE_LOWER);

$normalizedHeader = [];
foreach ($rawHeader as $idx => $col) {
    $lk = strtolower(trim($col));
    if ($lk === 'id') {
        $normalizedHeader[] = '__ID__';
        continue;
    }
    $normalizedHeader[] = $lcHeaderMap[$lk] ?? trim($col);
}

// === Build Records ===
$records = [];
$skipped = 0;

foreach ($lines as $lineIndex => $line) {
    $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
    if (count($fields) !== count($normalizedHeader)) {
        $msg = "Mismatched column count";
        fwrite(STDERR, "⚠️ $msg: " . json_encode($fields) . "\n");
        log_audit($fp,$lineIndex+2,"","","",$msg,"Skipped");
        $skipped++;
        continue;
    }

    $row = array_combine($normalizedHeader, $fields);

    if ($migrationType === "update" && empty($row["__ID__"])) {
        $msg = "Missing ID for update";
        fwrite(STDERR, "⚠️ $msg\n");
        log_audit($fp,$lineIndex+2,$row["firstName"]??"",$row["lastName"]??"",$row["email"]??"",$msg,"Skipped");
        $skipped++;
        continue;
    }

    if ($migrationType === "insert" && (
        empty(trim($row["firstName"] ?? "")) || empty(trim($row["email"] ?? ""))
    )) {
        $msg = "Missing mandatory fields (firstName/email)";
        fwrite(STDERR, "⚠️ $msg\n");
        log_audit($fp,$lineIndex+2,$row["firstName"]??"",$row["lastName"]??"",$row["email"]??"",$msg,"Skipped");
        $skipped++;
        continue;
    }

    // Resolve roles
    $resolvedRoles = [];
    $rawRoles = $row["System Role"] ?? "";
    $normalizedRoles = str_replace([";", "|"], ",", $rawRoles);
    $roleParts = array_map('trim', explode(",", $normalizedRoles));
    $unknownRoles = [];
    foreach ($roleParts as $role) {
        if ($role === "") continue;
        if (isset($roleMap[$role])) {
            $resolvedRoles[] = $roleMap[$role];
        } else {
            $unknownRoles[] = $role;
        }
    }
    if (!empty($unknownRoles)) {
        $msg = "Unknown role(s): " . implode(", ", $unknownRoles);
        fwrite(STDERR, "❌ $msg\n");
        log_audit($fp,$lineIndex+2,$row["firstName"]??"",$row["lastName"]??"",$row["email"]??"",$msg,"Skipped");
        $skipped++;
        continue;
    }

    try {
        $record = [
            "dataVersion" => 1,
            "stereotypeOperations" => [
                "Relate" => $resolvedRoles,
                "Unrelate" => []
            ],
            "SendOnboardingEmail" => !empty($row["sendonboardingemail"])
                ? filter_var($row["sendonboardingemail"], FILTER_VALIDATE_BOOLEAN)
                : false,
            "values" => [],
            "meta" => [
                "id" => $row["__ID__"] ?? null,
                "rowIndex" => count($records) + 2,
                "source" => $row
            ]
        ];

        if ($migrationType === "update" && !empty($row["__ID__"])) {
            $record["id"] = $row["__ID__"];
        }

        $record["values"]["userssourceid"]   = normalizeEmpty($row["userssourceid"] ?? "");
        $record["values"]["userstatus"]     = 0;
        $record["values"]["useLegacyLogin"] = false;
        $record["values"]["notes"]          = "";

        // Only allow fields that the API expects
        $allowedKeys = [
            "userssourceid","firstName","lastName","position","department",
            "organisation","phone","mobile","email"
        ];

        foreach ($allowedKeys as $key) {
            if (array_key_exists($key, $row)) {
                $record["values"][$key] = normalizeEmpty($row[$key]);
            }
        }
        $records[] = $record;
        log_audit($fp,$lineIndex+2,$row["firstName"]??"",$row["lastName"]??"",$row["email"]??"","Success","Success");

    } catch (Exception $e) {
        $msg = "Exception: " . $e->getMessage();
        fwrite(STDERR, "❌ $msg\n");
        log_audit($fp,$lineIndex+2,$row["firstName"]??"",$row["lastName"]??"",$row["email"]??"",$msg,"Skipped");
        $skipped++;
        continue;
    }
}

fclose($fp);
fwrite(STDERR, "🧾 Audit log written to $auditFile\n");

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "adapter_key" => "users",
    "records" => $records
];

fwrite(STDERR, "⚠️ Skipped {$skipped} invalid rows\n");
$json = json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
if ($json === false) {
    fwrite(STDERR, "❌ JSON encoding failed: " . json_last_error_msg() . "\n");
    exit(1);
}

// Save JSON payload into auditreports with timestamped name
$payloadFile = $reportDir . "/payload_users_" . date("Ymd_His") . ".json";
file_put_contents($payloadFile, $json);

echo $json . "\n";
fwrite(STDERR, "🧾 Payload written to $payloadFile\n");
fwrite(STDERR, "🧾 Writing detailed audit to $auditFile\n");