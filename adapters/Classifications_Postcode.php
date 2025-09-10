<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Adapter started\n");

// === CLI Arguments ===
$inputPath = $argv[1] ?? null;
$root = $argv[2] ?? null;

if (!is_numeric($root)) {
    throw new Exception("Invalid root hierarchy number");
}

// === Debug: Inspect Root and Hierarchy Structure ===
fwrite(STDERR, "📌 Root hierarchy number: $root\n");

$exampleHierarchy = "/$root/1/";
fwrite(STDERR, "🧪 Example hierarchy: $exampleHierarchy\n");

$segments = explode("/", trim($exampleHierarchy, "/"));
fwrite(STDERR, "🔍 Segments: " . implode(", ", $segments) . "\n");

$rootSegment = $segments[0] ?? 'undefined';
$childSegment = $segments[1] ?? 'undefined';
fwrite(STDERR, "📎 Root segment: $rootSegment\n");
fwrite(STDERR, "📎 Child segment: $childSegment\n");

try {
    // === Normalize Input Path ===
    $rawPath = $argv[1];
    $inputPath = trim($rawPath, " \t\n\r\0\x0B\"'");

    fwrite(STDERR, "🔍 Raw path: $rawPath\n");
    fwrite(STDERR, "📁 Normalized path: $inputPath\n");
    fwrite(STDERR, "📏 Length: " . strlen($inputPath) . "\n");

    if (!file_exists($inputPath)) {
        echo json_encode(["error" => "Input file not found: '$inputPath'"]);
        exit(1);
    }

    $handle = @fopen($inputPath, "r");
    if (!$handle) {
        $error = error_get_last();
        echo json_encode(["error" => "File cannot be opened", "details" => $error['message'] ?? 'Unknown']);
        exit(1);
    }
    fclose($handle);

    // === Load CSV ===
    $rows = array_map(function($line) {
        return str_getcsv($line, ",", '"', "\\");
    }, file($inputPath));

    // === Header Mapping ===
    $headerMap = [
        "Group ID"     => "id",
        "Name"         => "name",
        "Parent ID"    => "parent_id",
        "Hierarchy"    => "hierarchyLevel",
        "Description"  => "description"
    ];

    // === Load and Normalize Header ===
    $rawHeader = array_map('trim', array_shift($rows));
    $rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM
    $normalizedHeader = array_map(function($col) use ($headerMap) {
        return $headerMap[$col] ?? $col;
    }, $rawHeader);

    fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");
    fwrite(STDERR, "✅ Normalized header: " . implode(", ", $normalizedHeader) . "\n");

    // === Validate Required Columns ===
    $expected = ["id", "name", "parent_id", "hierarchyLevel", "description"];
    $missing = array_diff($expected, $normalizedHeader);
    if (!empty($missing)) {
        echo json_encode(["error" => "Missing columns: " . implode(", ", $missing)]);
        exit(1);
    }

    // === Process Rows ===
    $records = [];
    foreach ($rows as $row) {
        $record = array_combine($normalizedHeader, $row);

        // === Assign Variables ===
        $id             = trim($record["id"]);
        $name           = trim($record["name"]);
        $parentId       = trim($record["parent_id"]);
        $hierarchyLevel = intval(trim($record["hierarchyLevel"]));
        $description    = trim($record["description"]);

        // === Build Hierarchy String ===
        $hierarchy = $parentId !== "" ? "/$parentId/$id/" : "/$id/";
        $hierarchyLevel = substr_count($hierarchy, "/") - 2;

        // === Determine Classification Type ===
        $classificationType = ($parentId === "") ? "Postcode" : null;

        // === Build Output Record ===
        $group = [
            "classificationType" => $classificationType,
            "deleted"            => false,
            "description"        => $description !== "" ? $description : null,
            "hierarchy"          => $hierarchy,
            "hierarchyLevel"     => $hierarchyLevel,
            "name"               => $name
        ];

        $records[] = $group;
    }

    // === Output JSON ===
    $output = [
        "recordCount" => count($records),
        "generatedAt" => date("c"),
        "records"     => $records
    ];

    // Write to debug file (for inspection)
    file_put_contents("adapter_output_logs.json", json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

    // Echo to stdout for API consumption
    echo json_encode($output, JSON_UNESCAPED_SLASHES);
    fwrite(STDERR, "✅ Adapter completed\n");

} catch (Throwable $e) {
    $errorType = get_class($e);
    echo json_encode([
        "error"   => "Unhandled exception",
        "type"    => $errorType,
        "details" => $e->getMessage()
    ]);
    exit(1);
}