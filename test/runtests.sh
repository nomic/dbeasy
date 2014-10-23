SCRIPT_DIR=${0%/*}

foreman run mocha --timeout 5000 --ui tdd --reporter spec $SCRIPT_DIR/test*.js
