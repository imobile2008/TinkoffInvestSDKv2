## API Token Configuration

### Security Notice
**IMPORTANT**: API tokens contain sensitive credentials. Never commit tokens to version control.

### Setup Instructions

1. **Create token file** (copy from template):
   ```bash
   cp .test_token.txt.template .test_token.txt
   ```

2. **Add your Tinkoff Invest API token** to `.test_token.txt`:
   ```
   t.your_actual_token_here
   ```

3. **Or use environment variable** (alternative method):
   ```bash
   export TINKOFF_TOKEN="t.your_actual_token_here"
   ```

### Token Priority
The tests will look for the token in this order:
1. Token file (`.test_token.txt`) in project root
2. Environment variable (`TINKOFF_TOKEN`)
3. If neither is found, a warning is displayed and tests may fail

### .gitignore
The `.test_token.txt` file is already in `.gitignore` and will not be committed.

