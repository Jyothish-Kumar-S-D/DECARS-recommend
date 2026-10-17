# scripts/init.ps1
# Run ONCE after `docker compose up -d` on Windows PowerShell to:
#   1. Create HDFS directory structure
#   2. Upload product catalogue to HDFS
#   3. Create Kafka topics with correct partitions

Write-Host "`nWaiting for NameNode to be ready..." -ForegroundColor Yellow
$namenodeReady = $false
while (-not $namenodeReady) {
    try {
        $res = Invoke-WebRequest -Uri "http://localhost:9870" -UseBasicParsing -TimeoutSec 3 -ErrorAction SilentlyContinue
        if ($res.StatusCode -eq 200) { $namenodeReady = $true }
    } catch {
        Start-Sleep -Seconds 3
    }
}
Write-Host "NameNode is up" -ForegroundColor Green

Write-Host "Creating HDFS directories..." -ForegroundColor Yellow
docker exec namenode hdfs dfs -mkdir -p /data/clickstream
docker exec namenode hdfs dfs -mkdir -p /data/recommendations
docker exec namenode hdfs dfs -mkdir -p /models
docker exec namenode hdfs dfs -mkdir -p /checkpoints
docker exec namenode hdfs dfs -chmod -R 777 /data /models /checkpoints
Write-Host "HDFS directories created" -ForegroundColor Green

Write-Host "Uploading product catalogue to HDFS..." -ForegroundColor Yellow
$productsJson = @"
{"id":1,"name":"Sony WH-1000XM5","category":"Electronics","price":349,"rating":4.8}
{"id":2,"name":"MacBook Air M3","category":"Computers","price":1299,"rating":4.9}
{"id":3,"name":"Nike Air Max 270","category":"Footwear","price":130,"rating":4.6}
{"id":4,"name":"Kindle Paperwhite","category":"Electronics","price":139,"rating":4.7}
{"id":5,"name":"Instant Pot Duo 7-in-1","category":"Kitchen","price":99,"rating":4.8}
{"id":6,"name":"Levi's 511 Slim Jeans","category":"Apparel","price":79,"rating":4.5}
{"id":7,"name":"Fitbit Charge 6","category":"Wearables","price":159,"rating":4.4}
{"id":8,"name":"Canon EOS R50","category":"Photography","price":679,"rating":4.7}
{"id":9,"name":"LEGO Technic F40","category":"Toys","price":189,"rating":4.9}
{"id":10,"name":"Dyson V15 Detect","category":"Home","price":649,"rating":4.6}
{"id":11,"name":"Vitamix 5200","category":"Kitchen","price":449,"rating":4.8}
{"id":12,"name":"Samsung 4K QLED 65","category":"Electronics","price":1199,"rating":4.7}
"@

$tempFile = [System.IO.Path]::GetTempFileName()
[System.IO.File]::WriteAllText($tempFile, $productsJson)
docker cp $tempFile namenode:/tmp/products.json
docker exec namenode hdfs dfs -put -f /tmp/products.json /data/products.json
Remove-Item $tempFile -Force
Write-Host "Catalogue uploaded to hdfs:///data/products.json" -ForegroundColor Green

Write-Host "Waiting for Kafka broker..." -ForegroundColor Yellow
$kafkaReady = $false
while (-not $kafkaReady) {
    $out = docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list 2>&1
    if ($LASTEXITCODE -eq 0) {
        $kafkaReady = $true
    } else {
        Start-Sleep -Seconds 3
    }
}
Write-Host "Kafka is up" -ForegroundColor Green

Write-Host "📨 Creating Kafka topics..." -ForegroundColor Yellow
docker exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --create --if-not-exists `
  --topic clickstream `
  --partitions 12 `
  --replication-factor 1

docker exec kafka kafka-topics `
  --bootstrap-server kafka:29092 `
  --create --if-not-exists `
  --topic recommendations `
  --partitions 4 `
  --replication-factor 1
Write-Host "Kafka topics created" -ForegroundColor Green

Write-Host @"

═══════════════════════════════════════════════════
Cluster initialised!

Services:
  Frontend      →  http://localhost:3000
  Backend API   →  http://localhost:8000/docs
  HDFS Web UI   →  http://localhost:9870
  Spark UI      →  http://localhost:8082
  Kafka UI      →  http://localhost:8080

  Next step: POST http://localhost:8000/api/train
  (waits ~2 min for enough clickstream data first)
═══════════════════════════════════════════════════
"@ -ForegroundColor Cyan