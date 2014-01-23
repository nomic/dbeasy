SCRIPT_DIR=${0%/*}

foreman run mocha --ui tdd --reporter spec $SCRIPT_DIR/test.js
