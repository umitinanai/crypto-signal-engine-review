<!-- missing_06_mainnet_docs_part2.md — Part 2/2 — 1 files -->
<!-- Contents of this part: -->
<!--   - start_bot.sh (1805 bytes) -->

=== FILE: start_bot.sh ===
#!/bin/bash
# Sinyal botunu doğru ortam değişkenleriyle başlatır.
#
# Bu script, TESTNET signal-bridge/lifecycle'ının açık kalması için gereken
# ÜÇ config bayrağını (gizli olmayan) gömer. Ama iki GERÇEK KİMLİK BİLGİSİ
# (BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET) bu dosyada YOK ve
# OLMAYACAK — proje tasarımı gereği (SAFETY_INVARIANTS.md) hiçbir dosyada
# saklanmazlar. Bu scripti çalıştırmadan ÖNCE, AYNI terminalde, kendi
# elinle şunları export etmiş olman gerekir:
#
#   export BINANCE_TESTNET_API_KEY=...
#   export BINANCE_TESTNET_API_SECRET=...
#
# Eğer bunlar set değilse script aşağıda seni uyarıp DURACAK, sessizce
# PAPER-only moda düşmeyecek.
#
# Kullanım (proje kök dizininde):
#   chmod +x start_bot.sh   (sadece ilk seferde)
#   export BINANCE_TESTNET_API_KEY=...
#   export BINANCE_TESTNET_API_SECRET=...
#   ./start_bot.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ -z "${BINANCE_TESTNET_API_KEY:-}" || -z "${BINANCE_TESTNET_API_SECRET:-}" ]]; then
  echo "HATA: BINANCE_TESTNET_API_KEY ve/veya BINANCE_TESTNET_API_SECRET set değil."
  echo "Bu ikisini export ETMEDEN bu scripti çalıştırma — aksi halde bot"
  echo "sessizce TESTNET bridge olmadan (basit PAPER modda) başlar."
  echo ""
  echo "  export BINANCE_TESTNET_API_KEY=..."
  echo "  export BINANCE_TESTNET_API_SECRET=..."
  echo "  ./start_bot.sh"
  exit 1
fi

export CSE_EXECUTION_MODE=BINANCE_SPOT_TESTNET
export CSE_ENABLE_TESTNET_EXECUTION=true
export CSE_ENABLE_SIGNAL_TESTNET_BRIDGE=true

echo "Başlatılıyor — CSE_EXECUTION_MODE=$CSE_EXECUTION_MODE CSE_ENABLE_TESTNET_EXECUTION=$CSE_ENABLE_TESTNET_EXECUTION CSE_ENABLE_SIGNAL_TESTNET_BRIDGE=$CSE_ENABLE_SIGNAL_TESTNET_BRIDGE"
exec .venv/bin/python -m crypto_signal_engine.app run


