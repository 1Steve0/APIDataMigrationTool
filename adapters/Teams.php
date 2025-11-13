<?php
ini_set('display_errors', 0);
ini_set('log_errors', 1);
ini_set('error_log', 'php://stderr');
error_reporting(E_ALL & ~E_DEPRECATED & ~E_WARNING);

$mode = strtolower($argv[2] ?? 'insert'); // default to insert

function normalizeEmpty($value) {
    return is_null($value) || (is_string($value) && trim($value) === "") ? "" : $value;
}
function hasColumn($key, $normalizedHeader) {
    return in_array($key, $normalizedHeader);
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

// === Load Team Lookup ===
$lookup_path = "C:\\Users\\steve\\OneDrive\\Documents\\Social Pinpoint\\Project\\SWC\\CM ID Lookup\\Teams.csv";
$lookup_map = [];

if (($handle = fopen($lookup_path, "r")) !== false) {
    $header = fgetcsv($handle);
    $header[0] = preg_replace('/^\xEF\xBB\xBF/', '', $header[0]);
    while (($data = fgetcsv($handle)) !== false) {
        $row = array_combine($header, $data);
        $team_name = trim($row["Name"]);
        $team_id = trim($row["Id"]);
        if ($team_name !== "") {
            if (isset($lookup_map[$team_name])) {
                fwrite(STDERR, "⚠️ Duplicate team name in lookup: '{$team_name}'\n");
            }
            $lookup_map[$team_name] = $team_id;
        }
    }
    fclose($handle);
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
    "Name" => "name",
    "Description" => "description",
    "Projects" => "projects",
    "Source Id (Admin Only)" => "teamssourceid"
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]);

$normalizedHeader = array_map(function ($col) use ($headerMap) {
    return $headerMap[$col] ?? $col;
}, $rawHeader);

// === Validate Columns ===
$expected = array_values($headerMap);
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    fwrite(STDERR, "⚠️ Warning: Missing expected columns: " . implode(", ", $missing) . "\n");
}

// === Build Records ===
$records = [];
$timestamp = date("Y-m-d\TH:i:s");

try {
    foreach ($lines as $line) {
        $fields = array_map('trim', str_getcsv($line, ",", '"', "\\"));
        if (count($fields) !== count($normalizedHeader)) {
            fwrite(STDERR, "⚠️ Skipping row with mismatched column count: " . json_encode($fields) . "\n");
            continue;
        }

        $row = array_combine($normalizedHeader, $fields);
        if (!$row || !isset($row["name"])) {
            fwrite(STDERR, "⚠️ Skipping invalid row: " . json_encode($row) . "\n");
            continue;
        }

        // === Update mode: require ID ===
        $id = $mode === 'update' ? normalizeEmpty($row["id"] ?? "") : null;
        if ($mode === 'update' && !$id) {
            fwrite(STDERR, "⚠️ Skipping update row with missing ID\n");
            continue;
        }

        // === Resolve project IDs ===
        $relateIds = [];
        if (!empty($row["projects"])) {
            $projectNames = array_map('trim', explode(',', $row["projects"]));
            foreach ($projectNames as $projName) {
                if (isset($projectIds[$projName])) {
                    $relateIds[] = $projectIds[$projName];
                } else {
                    fwrite(STDERR, "⚠️ Unknown project name: '{$projName}'\n");
                }
            }
        }

        // === Build Payload ===
        $values = [];

        if (hasColumn("name", $normalizedHeader)) {
            $values["name"] = normalizeEmpty($row["name"] ?? "");
        }
        if (hasColumn("description", $normalizedHeader)) {
            $values["description"] = normalizeEmpty($row["description"] ?? "");
        }
        if (hasColumn("teamssourceid", $normalizedHeader)) {
            $values["teamssourceid"] = normalizeEmpty($row["teamssourceid"] ?? "");
        }
        if (hasColumn("teamssourceid", $normalizedHeader)) {
            $values["teamssourceid"] = normalizeEmpty($row["teamssourceid"] ?? "");
        }

        // Always include timestamps
        // $values["dateStart"] = $timestamp;
        // $values["dateEnd"] = $timestamp;

        $record = [
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

        $records[] = $record;
    }
} catch (Throwable $e) {
    fwrite(STDERR, "❌ Fatal error: " . $e->getMessage() . "\n");
    echo json_encode(["error" => "Adapter execution failed", "details" => $e->getMessage()]);
    exit(1);
}

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

file_put_contents("payload.json", $json);
echo $json . "\n";
