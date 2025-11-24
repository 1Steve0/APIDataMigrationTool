<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);
fwrite(STDERR, "🛠 Classifications adapter started (supports postcodes, stakeholder groups, distribution lists and more)\n");

// === Input Path ===
$inputPath = trim($argv[1] ?? '', " \t\n\r\0\x0B\"'");
if (!is_readable($inputPath)) {
    echo json_encode(["error" => "File not found or unreadable", "path" => $inputPath]);
    exit(1);
}

// === Load CSV ===
$lines = file($inputPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
fwrite(STDERR, "📦 Php file read:\n");

if (count($lines) < 2) {
    echo json_encode(["error" => "CSV file is empty or malformed"]);
    exit(1);
}

// === Header Mapping ===
$headerMap = [
    "parent_id"   => "parent_id",
    "name"        => "name",
    "description" => "description",
    "header"      => "header",
];

// === Normalize Header ===
$rawHeader = array_map('trim', str_getcsv(array_shift($lines), ",", '"', "\\"));
$rawHeader[0] = preg_replace('/^\xEF\xBB\xBF/', '', $rawHeader[0]); // Strip BOM
$normalizedHeader = array_map(fn($col) => $headerMap[$col] ?? $col, $rawHeader);

// === Validate Columns ===
fwrite(STDERR, "🧾 Raw header: " . implode(", ", $rawHeader) . "\n");
$expected = array_values($headerMap);
$missing = array_diff($expected, $normalizedHeader);
if ($missing) {
    echo json_encode(["error" => "Missing columns", "missing" => $missing]);
    exit(1);
}

// === Build Records ===
$records = [];
foreach ($lines as $index => $line) {
    try {
        $row = array_combine($normalizedHeader, array_map('trim', str_getcsv($line, ",", '"', "\\")));
        if (!$row) {
            fwrite(STDERR, "⚠️ Skipping malformed row: " . $line . "\n");
            $records[] = ["values" => [], "meta" => [
                "rowIndex"     => $index + 2,
                "adapter_name" => basename(__FILE__, ".php"),
                "raw"          => $line,
                "result"       => "Skipped",
                "message"      => "Malformed row"
            ]];
            continue;
        }
        if (!isset($row["name"]) || trim($row["name"]) === "") {
            fwrite(STDERR, "⚠️ Skipping row with empty name: " . json_encode($row) . "\n");
            $records[] = ["values" => [], "meta" => [
                "rowIndex"     => $index + 2,
                "adapter_name" => basename(__FILE__, ".php"),
                "raw"          => $line,
                "result"       => "Skipped",
                "message"      => "Empty name"
            ]];
            continue;
        }

        // remove the characters not allowed in this cell ie.  /,:;
        $row["name"] = preg_replace('/[\/:;]/', '', $row["name"]);

        // Normalize classificationType
        $classificationType = (strtoupper($row["header"]) === "TRUE") ? 1 : 2;

        // Build record
        $records[] = [
            "values" => [
                "classificationType" => $classificationType,
                "dataVersion"        => 0,
                "deleted"            => false,
                "description"        => $row["description"] ?: null,
                "name"               => $row["name"],
                "parentId"           => (int) $row["parent_id"]
            ],
            "meta" => [
                "rowIndex"     => $index + 2,
                "name"         => $row["name"],
                "parent_id"    => $row["parent_id"],
                "description"  => $row["description"] ?? "",
                "header"       => $row["header"] ?? "",
                "adapter_name" => basename(__FILE__, ".php"),
                "raw"          => $line,
                "result"       => "Success",
                "message"      => ""
            ]
        ];
    } catch (Throwable $e) {
        $records[] = ["values" => [], "meta" => [
            "rowIndex"     => $index + 2,
            "adapter_name" => basename(__FILE__, ".php"),
            "raw"          => $line,
            "result"       => "Error",
            "message"      => $e->getMessage()
        ]];
        continue;
    }
}

if (empty($records)) {
    fwrite(STDERR, "❌ No valid records generated\n");
    echo json_encode(["error" => "No valid records generated"]);
    exit(1);
}

// === Emit Output ===
$output = [
    "recordCount" => count($records),
    "generatedAt" => date("c"),
    "adapter_key" => "classifications",
    "records"     => $records
];

// === Output folder setup ===
$adapterName = "classifications";
$repoRoot    = dirname(__DIR__); // parent of current script folder
$reportDir   = $repoRoot . "/auditreports";
if (!is_dir($reportDir)) { mkdir($reportDir, 0777, true); }

$timestamp   = date("Ymd_His");
$auditFile   = $reportDir . "/migration_log_" . $adapterName . "_" . $timestamp . ".csv";
$payloadFile = $reportDir . "/payload_" . $adapterName . "_" . $timestamp . ".json";

// Write payload JSON
file_put_contents($payloadFile, json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

// === Write audit CSV ===
$fp = fopen($auditFile, "w");
fputcsv($fp, ["rowIndex", "name", "parent_id", "description", "header", "message", "result"], ",", '"', "\\");
foreach ($records as $rec) {
    fputcsv($fp, [
        $rec["meta"]["rowIndex"] ?? "",
        $rec["meta"]["name"] ?? "",
        $rec["meta"]["parent_id"] ?? "",
        $rec["meta"]["description"] ?? "",
        $rec["meta"]["header"] ?? "",
        $rec["meta"]["message"] ?? "",
        $rec["meta"]["result"] ?? "Success"
    ], ",", '"', "\\");
}
fclose($fp);

// === Write summary CSV ===
$summaryFile = $reportDir . "/migration_summary_" . $adapterName . "_" . $timestamp . ".csv";
$fpSummary   = fopen($summaryFile, "w");
fputcsv($fpSummary, ["recordCount", "successCount", "skippedCount", "errorCount", "generatedAt"], ",", '"', "\\");
$successCount = count(array_filter($records, fn($r) => ($r["meta"]["result"] ?? "") === "Success"));
$skippedCount = count(array_filter($records, fn($r) => ($r["meta"]["result"] ?? "") === "Skipped"));
$errorCount   = count(array_filter($records, fn($r) => ($r["meta"]["result"] ?? "") === "Error"));
fputcsv($fpSummary, [
    $output["recordCount"],
    $successCount,
    $skippedCount,
    $errorCount,
    $output["generatedAt"]
], ",", '"', "\\");
fclose($fpSummary);

fwrite(STDERR, "🧾 Summary written to $summaryFile\n");
fwrite(STDERR, "🧾 Payload written to $payloadFile\n");
fwrite(STDERR, "🧾 Audit log written to $auditFile\n");

echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
fwrite(STDERR, "✅ Adapter completed with {$output['recordCount']} records\n");