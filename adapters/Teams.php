<?php
ini_set('display_errors', 0);
ini_set('log_errors', 1);
ini_set('error_log', 'php://stderr');
error_reporting(E_ALL & ~E_DEPRECATED & ~E_WARNING);

$mode = strtolower($argv[2] ?? 'insert'); // insert | update

function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}
function hasColumn($key, $normalizedHeader) {
    return in_array($key, $normalizedHeader);
}
function transformProjects($rawProjects, $projectIds, $projectsTransform) {
    $relateIds = [];
    if (!empty($rawProjects)) {
        $tokens = preg_split('/[;,]/', $rawProjects); // split on , and ;
        foreach ($tokens as $token) {
            $proj = trim($token);
            if ($proj === "") continue;

            if (ctype_digit($proj)) { // numeric IDs direct
                $relateIds[] = (int)$proj;
                continue;
            }
            if (isset($projectIds[$proj])) { // name from CSV lookup
                $relateIds[] = $projectIds[$proj];
                continue;
            }
            if (isset($projectsTransform[$proj])) { // hard-coded map fallback
                $relateIds[] = $projectsTransform[$proj];
                continue;
            }
            fwrite(STDERR, "⚠️ Unknown project reference: '{$proj}'\n");
        }
    }
    return array_values(array_unique($relateIds));
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

// === Load Project IDs for Team assignment ===
$projectIdPath = "C:\\Users\\steve\\OneDrive\\Documents\\Social Pinpoint\\Project\\SWC\\CM ID Lookup\\Project.csv";
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

// === Header Mapping ===
$headerMap = [
    "Id" => "id",
    "Source Id (Admin Only)" => "teamssourceid",
    "Name" => "name",
    "Description" => "description",
    "Projects" => "projects"
];

// Hard-coded transforms (fallback names → IDs)
$projectsTransform = [
    "Glasshouse Mountains6" => 14,
    "Glasshouse Mountains5" => 13,
    "Manfield Road Upgrade2" => 12
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]);
$normalizedHeader = array_map(function ($col) use ($headerMap) { return $headerMap[$col] ?? $col; }, $rawHeader);

// === Validate Columns (warn-only)
$expected = array_values($headerMap);
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    fwrite(STDERR, "⚠️ Warning: Missing expected columns: " . implode(", ", $missing) . "\n");
}

// === Audit setup ===
$adapterName = "teams";
$repoRoot = dirname(__DIR__);              // one level up from adapters folder
$reportDir = $repoRoot . "/auditreports";  // same auditreports folder
if (!is_dir($reportDir)) { mkdir($reportDir, 0777, true); }

$auditFile = $reportDir . "/migration_log_" . $adapterName . "_" . date("Ymd_His") . ".csv";
$fp = fopen($auditFile, "w");
if ($fp === false) {
    fwrite(STDERR, "❌ Could not open audit log file: $auditFile\n");
    exit(1);
}
fputcsv($fp, ["rowIndex","mode","id","teamssourceid","name","description","projectsRelate"]);

// === Build Records ===
$records = [];
$rowIndex = 0;

try {
    foreach ($lines as $line) {
        $rowIndex++;
        $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
        if (count($fields) !== count($normalizedHeader)) {
            fwrite(STDERR, "⚠️ Skipping row {$rowIndex}: mismatched column count\n");
            continue;
        }

        $row = array_combine($normalizedHeader, $fields);
        if (!$row) {
            fwrite(STDERR, "⚠️ Skipping row {$rowIndex}: invalid row shape\n");
            continue;
        }

        // Update mode: require id
        $id = $mode === 'update' ? normalizeEmpty($row["id"] ?? "") : null;
        if ($mode === 'update' && !$id) {
            fwrite(STDERR, "⚠️ Skipping row {$rowIndex}: missing ID for update\n");
            continue;
        }

        // Resolve project IDs via transform
        $relateIds = transformProjects($row["projects"] ?? "", $projectIds, $projectsTransform);

        // Build Values
        $values = [];
        if (hasColumn("name", $normalizedHeader))          $values["name"] = normalizeEmpty($row["name"] ?? "");
        if (hasColumn("description", $normalizedHeader))   $values["description"] = normalizeEmpty($row["description"] ?? "");
        if (hasColumn("teamssourceid", $normalizedHeader)) $values["teamssourceid"] = normalizeEmpty($row["teamssourceid"] ?? "");

        // Emit meta for logging and ID handling
        $meta = [
            "rowIndex" => $rowIndex,
            "id" => $mode === 'update' ? $id : "",
            "teamssourceid" => $values["teamssourceid"] ?? "",
            "name" => $values["name"] ?? ""
        ];

        $record = [
            "meta" => $meta,
            "DataVersion" => 1,
            "ProjectOperations" => [
                "Relate" => $relateIds,
                "Unrelate" => []
            ],
            "Values" => $values
        ];

        if ($mode === 'update') {
            $record["id"] = $id;
        }

        fwrite(STDERR, "🔧 Row {$rowIndex} built (mode={$mode}): " . json_encode($record) . "\n");
        $records[] = $record;

        // Write to CSV audit log
        fputcsv($fp, [
            $rowIndex,
            $mode,
            $id ?? "",
            $values["teamssourceid"] ?? "",
            $values["name"] ?? "",
            $values["description"] ?? "",
            implode(";", $relateIds)
        ]);
    }
} catch (Throwable $e) {
    fwrite(STDERR, "❌ Fatal error: " . $e->getMessage() . "\n");
    echo json_encode(["error" => "Adapter execution failed", "details" => $e->getMessage()]);
    exit(1);
}

fclose($fp);
fwrite(STDERR, "🧾 Audit log written to $auditFile\n");

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "adapter_key"=> "teams",
    "records" => $records
];

$json = json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
if ($json === false) {
    $error = json_last_error_msg();
    fwrite(STDERR, "❌ JSON encoding failed: $error\n");
    echo json_encode(["error" => "JSON encoding failed", "details" => $error]);
    exit(1);
}

if (!is_array($records) || empty($records)) {
    fwrite(STDERR, "❌ No valid records generated\n");
}

// Save JSON payload into auditreports with timestamped name
$payloadFile = $reportDir . "/payload_" . $adapterName . "_" . date("Ymd_His") . ".json";
file_put_contents($payloadFile, $json);
fwrite(STDERR, "🧾 Payload written to $payloadFile\n");

echo $json . "\n";