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

// === Load Lookup Map ===
$lookup_path = "C:\\Users\\steve\\OneDrive\\Documents\\Social Pinpoint\\Project\\SWC\\CM ID Lookup\\Project.csv";
$lookup_map = [];

if (($handle = fopen($lookup_path, "r")) !== false) {
    $header = fgetcsv($handle);
    $header[0] = preg_replace('/^\xEF\xBB\xBF/', '', $header[0]);
    while (($data = fgetcsv($handle)) !== false) {
        $row = array_combine($header, $data);
        $group_name = trim($row["Import Path"]);
        $group_id = trim($row["Id"]);
        if ($group_name !== "") {
            if (isset($lookup_map[$group_name])) {
                fwrite(STDERR, "⚠️ Duplicate group label in lookup: '{$group_name}'\n");
            }
            $lookup_map[$group_name] = $group_id;
        }
    }
    fclose($handle);
}

// === Header Mapping ===
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

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]);

$normalizedHeader = array_map(function ($col) use ($headerMap) {
    $mapped = $headerMap[$col] ?? $col;
    return is_array($mapped) ? implode('.', $mapped) : $mapped;
}, $rawHeader);

// === Validate Columns ===
$expected = array_map(function ($v) {
    return is_array($v) ? implode('.', $v) : $v;
}, array_values($headerMap));
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
        if (!$row) {
            fwrite(STDERR, "⚠️ Skipping invalid row: " . json_encode($row) . "\n");
            continue;
        }

        if ($mode === 'insert' && empty(trim($row["name"] ?? ""))) {
            fwrite(STDERR, "⚠️ Skipping insert row missing mandatory field (name): " . json_encode($row) . "\n");
            continue;
        }

        // === Update mode: require ID ===
        $id = $mode === 'update' ? normalizeEmpty($row["id"] ?? "") : null;
        if ($mode === 'update' && !$id) {
            fwrite(STDERR, "⚠️ Skipping update row with missing ID\n");
            continue;
        }

        // === Group Lookup ===
        $projectGroupIntegers = [];
        if (!empty($row["projectGroup"])) {
            $group_labels = array_map('trim', explode(',', $row["projectGroup"]));
            foreach ($group_labels as $label) {
                if (isset($lookup_map[$label])) {
                    $projectGroupIntegers[] = intval($lookup_map[$label]);
                } else {
                    fwrite(STDERR, "⚠️ Unknown group label: '{$label}'\n");
                }
            }
        }
        $projectGroupIntegers = array_unique($projectGroupIntegers);

        // === Location Parsing ===
        $address_location = "";
        if (!empty($row["address.location"]) && strpos($row["address.location"], ",") !== false) {
            list($latitude, $longitude) = explode(",", $row["address.location"], 2);
            $address_location = [
                "latitude" => trim($latitude),
                "longitude" => trim($longitude),
                "type" => "Point"
            ];
        }

        // === Build Payload ===
        $values = [];

        if (hasColumn("name", $normalizedHeader)) {
            $values["name"] = normalizeEmpty($row["name"] ?? "");
        }
        if (hasColumn("notes", $normalizedHeader)) {
            $values["notes"] = normalizeEmpty($row["notes"] ?? "");
        }
        if (hasColumn("projectsourceid", $normalizedHeader)) {
            $values["projectsourceid"] = normalizeEmpty($row["projectsourceid"] ?? "");
        }
        if (hasColumn("timeZone", $normalizedHeader)) {
            $values["timeZone"] = normalizeEmpty($row["timeZone"] ?? "");
        }
        if (hasColumn("projectGroup", $normalizedHeader)) {
            $values["projectGroup"] = [
                "assign" => $projectGroupIntegers,
                "unassign" => []
            ];
        }

        // === Address block ===
        $address = [];
        foreach (["address.address", "address.suburb", "address.state", "address.postCode", "address.country"] as $field) {
            if (hasColumn($field, $normalizedHeader)) {
                $address[explode(".", $field)[1]] = normalizeEmpty($row[$field] ?? "");
            }
        }
        if (hasColumn("address.location", $normalizedHeader)) {
            $address["location"] = $address_location;
        }
        if (hasColumn("address.autoGeocode", $normalizedHeader)) {
            $address["autoGeocode"] = filter_var($row["address.autoGeocode"] ?? false, FILTER_VALIDATE_BOOLEAN);
        }
        if (!empty($address)) {
            $values["address"] = $address;
        }

        // Always include timestamps
        $values["dateStart"] = $timestamp;
        $values["dateEnd"] = $timestamp;

        $record = [
            "dataVersion" => 1,
            "Values" => $values
        ];
        if ($mode === 'update') { //Update
            $record["id"] = $id;
        }
        else { // Insert mode
            $record["id"] = $id;
            $record["projectOperations"] = [
                "Relate" => [],
                "Unrelate" => []
            ];
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
    "valueKey" => "Values",
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
fwrite(STDERR, "📁 Wrote insert rows to: " . realpath($insertPath) . "\n");
fwrite(STDERR, "📁 Wrote update rows to: " . realpath($updatePath) . "\n");