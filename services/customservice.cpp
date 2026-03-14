#include "customservice.h"
#include "commontypes.h"

CustomService::CustomService(const std::string &token) : m_token(token)
{

}

std::shared_ptr<grpc::ClientContext> CustomService::makeContext()
{
    auto context = std::make_shared<grpc::ClientContext>();
    std::string meta_value = "Bearer " + m_token;
    context->AddMetadata("authorization", meta_value);
    context->AddMetadata("x-app-name", APP_NAME);
    return context;
}




