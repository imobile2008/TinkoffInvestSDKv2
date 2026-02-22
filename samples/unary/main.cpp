#include "investapiclient.h"
#include "sandboxservice.h"
#include "marketdataservice.h"
#include "usersservice.h"
#include "ordersservice.h"
#include <fstream>
#include <sstream>
#include <vector>
#include <iostream>

// Helper function to load token from file with fallback to environment variable
std::string loadToken()
{
    // Try multiple possible locations for the token file
    std::vector<std::string> possiblePaths = {
        ".test_token.txt",
        "../.test_token.txt",
        "../../.test_token.txt",
        "../../../.test_token.txt"
    };
    
    for (const auto& filename : possiblePaths) {
        std::ifstream file(filename);
        if (file.is_open()) {
            std::string token;
            std::getline(file, token);
            // Remove any whitespace or newline characters
            token.erase(remove_if(token.begin(), token.end(), ::isspace), token.end());
            if (!token.empty()) {
                std::cout << "[DEBUG] Loaded token from: " << filename << std::endl;
                return token;
            }
            file.close();
        }
    }
    
    // Fallback to environment variable
    const char* envToken = getenv("TOKEN");
    if (envToken != nullptr) {
        std::cout << "[DEBUG] Loaded token from environment variable" << std::endl;
        return std::string(envToken);
    }
    
    std::cerr << "[ERROR] Could not find token file or TOKEN environment variable!" << std::endl;
    // Return empty string if no token found
    return "";
}

int main()
{
    InvestApiClient client("invest-public-api.tinkoff.ru:443", loadToken());

    //get references to sandbox and marketdata services
    auto sandbox = std::dynamic_pointer_cast<Sandbox>(client.service("sandbox"));
    auto marketdata = std::dynamic_pointer_cast<MarketData>(client.service("marketdata"));

    //print last prices of X5 RetailGroup and Sberbank
    auto prices = marketdata->GetLastPrices({"BBG00JXPFBN0", "BBG0047315Y7"});
    if (prices.GetStatus().ok() && prices.ptr() != nullptr) {
        std::cout << prices.ptr()->DebugString() << std::endl;
    } else {
        std::cerr << "Failed to get last prices: " << prices.GetErrorMessage() << std::endl;
    }

    //open account
    sandbox->OpenSandboxAccount();

    //print all opened accounts id
    auto accounts = sandbox->GetSandboxAccounts();
    for (int i = 0; i < accounts.accountCount(); i++)
        std::cout << accounts.accountID(i) << accounts.accountName(i) << std::endl;

    //print info about your account
    auto accountId = accounts.accountID(0);
    auto portfolio = sandbox->GetSandboxPortfolio(accountId, PortfolioRequest_CurrencyRequest_RUB);
    if (portfolio.GetStatus().ok() && portfolio.ptr() != nullptr) {
        std::cout << portfolio.ptr()->DebugString() << std::endl;
    } else {
        std::cerr << "Failed to get portfolio: " << portfolio.GetErrorMessage() << std::endl;
    }

    //close account
    sandbox->CloseSandboxAccount(accountId);

    return 0;
}
