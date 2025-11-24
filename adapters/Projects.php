<?php
ini_set('display_errors', 0);
ini_set('log_errors', 1);
ini_set('error_log', 'php://stderr');
error_reporting(E_ALL & ~E_DEPRECATED & ~E_WARNING);

$mode = strtolower($argv[2] ?? 'insert');

function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}
function hasColumn($key, $normalizedHeader) { return in_array($key, $normalizedHeader); }
function log_audit($fp,$idx,$name,$group,$msg,$result){ fputcsv($fp,[$idx,$name??"",$group??"",$msg,$result]); }

// Input path
$inputPath = trim($argv[1] ?? '', " \t\n\r\0\x0B\"'");
if (!is_readable($inputPath)) {
    echo json_encode(["error" => "File not found or unreadable", "path" => $inputPath]);
    exit(1);
}

// Load CSV
$lines = file($inputPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
if (count($lines) < 2) {
    echo json_encode(["error" => "CSV file is empty or malformed"]);
    exit(1);
}

// Lookup map
$lookup_map = [
    "North:Project Group1" => 5330,
    "South:Project Group2" => 5337,
    "North:Project Group3" => 5341
];

// Header mapping
$headerMap = [
    "Id" => "id",
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
    "Source Id (Admin Only)" => "projectsourceid"
];

// Normalize header
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]);
$normalizedHeader = array_map(function ($col) use ($headerMap) {
    $mapped = $headerMap[$col] ?? $col;
    return is_array($mapped) ? implode('.', $mapped) : $mapped;
}, $rawHeader);

// Validate columns
$expected = array_map(function ($v) { return is_array($v) ? implode('.', $v) : $v; }, array_values($headerMap));
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    fwrite(STDERR, "⚠️ Warning: Missing expected columns: " . implode(", ", $missing) . "\n");
}

// Build records
$records = [];
$timestamp = date("Y-m-d\TH:i:s");

try {
    $adapterName = "projects";
    // Resolve repo root as the parent of the current script folder
    $repoRoot = dirname(__DIR__);  // one level up from adapters folder
    $reportDir = $repoRoot . "/auditreports";

    if (!is_dir($reportDir)) {
        mkdir($reportDir, 0777, true);
    }
    if (!is_dir($reportDir)) { mkdir($reportDir, 0777, true); }
    $auditFile = $reportDir . "/migration_log_" . $adapterName . "_" . date("Ymd_His") . ".csv";
    $fp = fopen($auditFile, "w");
    fputcsv($fp, ["rowIndex", "name", "projectGroup", "message", "result"]);

    foreach ($lines as $lineIndex => $line) {
        $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
        if (count($fields) !== count($normalizedHeader)) {
            $msg = "Skipping row with mismatched column count";
            fwrite(STDERR, "⚠️ $msg: " . json_encode($fields) . "\n");
            log_audit($fp, $lineIndex+2, "", "", $msg, "Skipped");
            continue;
        }

        $row = array_combine($normalizedHeader, $fields);
        if (!$row) {
            $msg = "Invalid row";
            fwrite(STDERR, "⚠️ $msg\n");
            log_audit($fp, $lineIndex+2, "", "", $msg, "Skipped");
            continue;
        }

        // Group lookup
        $projectGroupIntegers = [];
        $warnings = [];
        $invalidGroup = false;
        if (!empty($row["projectGroup"])) {
            foreach (array_map('trim', explode(',', $row["projectGroup"])) as $label) {
                if ($label === "") continue;
                if (ctype_digit($label)) { $projectGroupIntegers[] = intval($label); }
                elseif (isset($lookup_map[$label])) { $projectGroupIntegers[] = intval($lookup_map[$label]); }
                else {
                    $invalidGroup = true;
                    $warnings[] = "Lookup_map value '{$label}' not found";
                    fwrite(STDERR, "⚠️ Lookup_map value '{$label}' not found\n");
                }
            }
        }
        $projectGroupIntegers = array_unique($projectGroupIntegers);

        if ($invalidGroup) {
            log_audit($fp, $lineIndex+2, $row["name"] ?? "", $row["projectGroup"] ?? "", implode("; ", $warnings), "Skipped");
            continue;
        }

        // Build payload
        $values = [];
        foreach (["name","notes","projectsourceid","timeZone"] as $field)
            if (hasColumn($field, $normalizedHeader)) $values[$field] = normalizeEmpty($row[$field] ?? "");

        if (hasColumn("projectGroup", $normalizedHeader))
            $values["projectGroup"] = ["assign" => $projectGroupIntegers, "unassign" => []];

        // Address block - Flattened address fields directly into values
        $address = [];
        foreach (["address.address", "address.suburb", "address.state", "address.postCode", "address.country"] as $field) {
            if (hasColumn($field, $normalizedHeader)) {
                $values[$field] = normalizeEmpty($row[$field] ?? "");
            }
        }

        if (hasColumn("address.location", $normalizedHeader)) {
            $values["address.location"] = normalizeEmpty($row["address.location"] ?? "");
        }
        
        // Address.location handling: split combined "lat,long" string into structured object
        if (hasColumn("address.location", $normalizedHeader)) {
            $loc = normalizeEmpty($row["address.location"] ?? "");
            if ($loc !== "" && strpos($loc, ",") !== false) {
                list($lat, $lon) = explode(",", $loc, 2);
                $values["address.location"] = [
                    "latitude" => trim($lat),
                    "longitude" => trim($lon),
                    "type" => "Point"
                ];
            } else {
                // fallback: keep as string if not in "lat,long" format
                $values["address.location"] = $loc;
            }
        }

        if (hasColumn("address.autoGeocode", $normalizedHeader)) {
            $values["address.autoGeocode"] = filter_var($row["address.autoGeocode"] ?? false, FILTER_VALIDATE_BOOLEAN);
        }
        if (!empty($address)) $values["address"] = $address;


        // Timestamps
        $values["dateStart"] = $timestamp;
        $values["dateEnd"] = $timestamp;

        // Skip record creation if missing name (optional)
        if (empty($values["name"])) {
            log_audit($fp, $lineIndex+2, "", implode(",", $projectGroupIntegers), "Missing mandatory field (name)", "Skipped");
            continue;
        }

        $record = ["dataVersion" => 1, "values" => $values];

        if ($mode === "update" && hasColumn("id", $normalizedHeader)) {
            $record["meta"] = ["id" => normalizeEmpty($row["id"] ?? "")];
        }
        $records[] = $record;

        // Log success
        log_audit($fp, $lineIndex+2, $values["name"] ?? "", implode(",", $projectGroupIntegers), "", "Success");
    }

    fclose($fp);
    fwrite(STDERR, "🧾 Audit log written to $auditFile\n");

} catch (Throwable $e) {
    fwrite(STDERR, "❌ Fatal error: " . $e->getMessage() . "\n");
    echo json_encode(["error" => "Adapter execution failed", "details" => $e->getMessage()]);
    exit(1);
}

// Emit output
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "adapter_key"=> "projects",
    "records" => $records
];

$json = json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
if ($json === false) {
    $error = json_last_error_msg();
    fwrite(STDERR, "❌ JSON encoding failed: $error\n");
    echo json_encode(["error" => "JSON encoding failed", "details" => $error]);
    exit(1);
}
if (!is_array($records) || empty($records)) { fwrite(STDERR, "❌ No valid records generated\n"); }

// Save JSON payload into auditreports with timestamped name
$payloadFile = $reportDir . "/payload_projects_" . date("Ymd_His") . ".json";
file_put_contents($payloadFile, $json);

echo $json . "\n";
fwrite(STDERR, "🧾 Payload written to $payloadFile\n");
fwrite(STDERR, "🧾 Writing detailed audit to $auditFile\n");