#include "instrumentsservice.h"

Instruments::Instruments(std::shared_ptr<grpc::Channel> channel, const std::string &token) :
    CustomService(token),
    m_instrumentsService(InstrumentsService::NewStub(channel))
{

}

Instruments::~Instruments()
{

}

ServiceReply Instruments::TradingSchedules(const std::string &exchange, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
{
    TradingSchedulesRequest request;
    request.set_exchange(exchange);
    google::protobuf::Timestamp * from = new google::protobuf::Timestamp();
    from->set_seconds(fromseconds);
    from->set_nanos(fromnanos);
    request.set_allocated_from(from);
    google::protobuf::Timestamp * to = new google::protobuf::Timestamp();
    to->set_seconds(toseconds);
    to->set_nanos(tonanos);
    request.set_allocated_to(to);
    TradingSchedulesResponse reply;
    Status status = m_instrumentsService->TradingSchedules(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<TradingSchedulesResponse>(status, reply, exchange);
}

ServiceReply Instruments::BondBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    InstrumentRequest request;
    request.set_id_type(idType);
    request.set_class_code(classCode);
    request.set_id(id);
    BondResponse reply;
    Status status = m_instrumentsService->BondBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<BondResponse>(status, reply);
}

ServiceReply Instruments::Bonds(InstrumentStatus instrumentStatus)
{
    InstrumentsRequest request;
    request.set_instrument_status(instrumentStatus);
    BondsResponse reply;
    Status status = m_instrumentsService->Bonds(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<BondsResponse>(status, reply);
}

ServiceReply Instruments::GetBondCoupons(const std::string  &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
{
    GetBondCouponsRequest request;
    request.set_figi(figi);
    google::protobuf::Timestamp * from = new google::protobuf::Timestamp();
    from->set_seconds(fromseconds);
    from->set_nanos(fromnanos);
    request.set_allocated_from(from);
    google::protobuf::Timestamp * to = new google::protobuf::Timestamp();
    to->set_seconds(toseconds);
    to->set_nanos(tonanos);
    request.set_allocated_to(to);
    GetBondCouponsResponse reply;
    Status status = m_instrumentsService->GetBondCoupons(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetBondCouponsResponse>(status, reply);
}

ServiceReply Instruments::CurrencyBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    InstrumentRequest request;
    request.set_id_type(idType);
    request.set_class_code(classCode);
    request.set_id(id);
    CurrencyResponse reply;
    Status status = m_instrumentsService->CurrencyBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<CurrencyResponse>(status, reply);
}

ServiceReply Instruments::Currencies(InstrumentStatus instrumentStatus)
{
    InstrumentsRequest request;
    request.set_instrument_status(instrumentStatus);
    CurrenciesResponse reply;
    Status status = m_instrumentsService->Currencies(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<CurrenciesResponse>(status, reply);
}

ServiceReply Instruments::EtfBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    InstrumentRequest request;
    request.set_id_type(idType);
    request.set_class_code(classCode);
    request.set_id(id);
    EtfResponse reply;
    Status status = m_instrumentsService->EtfBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<EtfResponse>(status, reply);
}

ServiceReply Instruments::Etfs(InstrumentStatus instrumentStatus)
{
    InstrumentsRequest request;
    request.set_instrument_status(instrumentStatus);
    EtfsResponse reply;
    Status status = m_instrumentsService->Etfs(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<EtfsResponse>(status, reply);
}

ServiceReply Instruments::FutureBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    InstrumentRequest request;
    request.set_id_type(idType);
    request.set_class_code(classCode);
    request.set_id(id);
    FutureResponse reply;
    Status status = m_instrumentsService->FutureBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<FutureResponse>(status, reply);
}

ServiceReply Instruments::Futures(InstrumentStatus instrumentStatus)
{
    InstrumentsRequest request;
    request.set_instrument_status(instrumentStatus);
    FuturesResponse reply;
    Status status = m_instrumentsService->Futures(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<FuturesResponse>(status, reply);
}

ServiceReply Instruments::ShareBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    InstrumentRequest request;
    request.set_id_type(idType);
    request.set_class_code(classCode);
    request.set_id(id);
    ShareResponse reply;
    Status status = m_instrumentsService->ShareBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<ShareResponse>(status, reply);
}

ServiceReply Instruments::Shares(InstrumentStatus instrumentStatus)
{
    InstrumentsRequest request;
    request.set_instrument_status(instrumentStatus);
    SharesResponse reply;
    Status status = m_instrumentsService->Shares(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<SharesResponse>(status, reply);
}

ServiceReply Instruments::GetAccruedInterests(const std::string  &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
{
    GetAccruedInterestsRequest request;
    request.set_figi(figi);
    google::protobuf::Timestamp * from = new google::protobuf::Timestamp();
    from->set_seconds(fromseconds);
    from->set_nanos(fromnanos);
    request.set_allocated_from(from);
    google::protobuf::Timestamp * to = new google::protobuf::Timestamp();
    to->set_seconds(toseconds);
    to->set_nanos(tonanos);
    request.set_allocated_to(to);
    GetAccruedInterestsResponse reply;
    Status status = m_instrumentsService->GetAccruedInterests(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetAccruedInterestsResponse>(status, reply);
}

ServiceReply Instruments::GetFuturesMargin(const std::string  &figi)
{
    GetFuturesMarginRequest request;
    request.set_figi(figi);
    GetFuturesMarginResponse reply;
    Status status = m_instrumentsService->GetFuturesMargin(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetFuturesMarginResponse>(status, reply);
}

ServiceReply Instruments::GetInstrumentBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    InstrumentRequest request;
    request.set_id_type(idType);
    request.set_class_code(classCode);
    request.set_id(id);
    InstrumentResponse reply;
    Status status = m_instrumentsService->GetInstrumentBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<InstrumentResponse>(status, reply);
}

ServiceReply Instruments::GetDividends(const std::string  &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
{
    GetDividendsRequest request;
    request.set_figi(figi);
    google::protobuf::Timestamp * from = new google::protobuf::Timestamp();
    from->set_seconds(fromseconds);
    from->set_nanos(fromnanos);
    request.set_allocated_from(from);
    google::protobuf::Timestamp * to = new google::protobuf::Timestamp();
    to->set_seconds(toseconds);
    to->set_nanos(tonanos);
    request.set_allocated_to(to);
    GetDividendsResponse reply;
    Status status = m_instrumentsService->GetDividends(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetDividendsResponse>(status, reply);
}

ServiceReply Instruments::GetAssetBy(const std::string  &id)
{
    AssetRequest request;
    request.set_id(id);
    AssetResponse reply;
    Status status = m_instrumentsService->GetAssetBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<AssetResponse>(status, reply);
}

ServiceReply Instruments::GetAssets()
{
    AssetsRequest request;
    AssetsResponse reply;
    Status status = m_instrumentsService->GetAssets(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<AssetsResponse>(status, reply);
}

ServiceReply Instruments::GetFavorites()
{
    GetFavoritesRequest request;
    GetFavoritesResponse reply;
    Status status = m_instrumentsService->GetFavorites(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetFavoritesResponse>(status, reply);
}

ServiceReply Instruments::EditFavorites(const std::vector<EditFavoritesRequestInstrument> &instruments, EditFavoritesActionType actionType)
{
    EditFavoritesRequest request;
    for (auto &instrument: instruments)
    {
        auto inst = request.add_instruments();
        inst->CopyFrom(instrument);
    }
    request.set_action_type(actionType);
    EditFavoritesResponse reply;
    Status status = m_instrumentsService->EditFavorites(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<EditFavoritesResponse>(status, reply);
}

ServiceReply Instruments::GetBondEvents(const std::string &figi, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
{
    // GetBondEventsRequest/GetBondEventsResponse not available in current API
    // GetBondEventsRequest request;
    // request.set_instrument_id(figi);
    // google::protobuf::Timestamp *from = new google::protobuf::Timestamp();
    // from->set_seconds(fromseconds);
    // from->set_nanos(fromnanos);
    // request.set_allocated_from(from);
    // google::protobuf::Timestamp *to = new google::protobuf::Timestamp();
    // to->set_seconds(toseconds);
    // to->set_nanos(tonanos);
    // request.set_allocated_to(to);
    // GetBondEventsResponse reply;
    // Status status = m_instrumentsService->GetBondEvents(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<GetBondEventsResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "GetBondEvents method not available in current API version");
}

ServiceReply Instruments::OptionBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    InstrumentRequest request;
    request.set_id_type(idType);
    request.set_class_code(classCode);
    request.set_id(id);
    OptionResponse reply;
    Status status = m_instrumentsService->OptionBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<OptionResponse>(status, reply);
}

ServiceReply Instruments::Options(InstrumentStatus instrumentStatus)
{
    InstrumentsRequest request;
    request.set_instrument_status(instrumentStatus);
    OptionsResponse reply;
    Status status = m_instrumentsService->Options(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<OptionsResponse>(status, reply);
}

// OptionsBy - uses FilterOptionsRequest from current API
// Note: optionType parameter not available in current API
// The method signature changed - optionType removed
ServiceReply Instruments::OptionsBy(InstrumentStatus instrumentStatus, const std::string &underlyingFigi, std::string &isoCurrency)
{
    // FilterOptionsRequest doesn't exist in current API - method disabled
    // FilterOptionsRequest request;
    // request.set_basic_asset_uid(underlyingFigi);
    // OptionsResponse reply;
    // Status status = m_instrumentsService->OptionsBy(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<OptionsResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "OptionsBy method not available in current API version");
}

ServiceReply Instruments::DfaBy(InstrumentIdType idType, const std::string &classCode, const std::string &id)
{
    // DfaBy method not available in current API
    // InstrumentRequest request;
    // request.set_id_type(idType);
    // request.set_class_code(classCode);
    // request.set_id(id);
    // DfaResponse reply;
    // Status status = m_instrumentsService->DfaBy(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<DfaResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "DfaBy method not available in current API version");
}

ServiceReply Instruments::Dfas(InstrumentStatus instrumentStatus)
{
    // Dfas method not available in current API
    // DfasRequest request;
    // DfasResponse reply;
    // Status status = m_instrumentsService->Dfas(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<DfasResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "Dfas method not available in current API version");
}

// ServiceReply Instruments::Indicatives(const std::string &instrumentId)
// {
//     // IndicativesRequest is empty in current API - no fields available
//     // IndicativesRequest request;
//     // IndicativesResponse reply;
//     // Status status = m_instrumentsService->Indicatives(makeContext().get(), request, &reply);
//     // return ServiceReply::prepareServiceAnswer<IndicativesResponse>(status, reply);
//     return ServiceReply(grpc::Status::CANCELLED, "Indicatives method not available in current API version");
// }

// ServiceReply Instruments::CreateFavoriteGroup(const std::string &name, const std::vector<std::string> &instrumentIds)
// {
//     // CreateFavoriteGroupRequest has different fields in current API
//     // CreateFavoriteGroupRequest request;
//     // request.set_name(name);
//     // for (const auto& id : instrumentIds) {
//     //     request.add_instrument_ids(id);
//     // }
//     // CreateFavoriteGroupResponse reply;
//     // Status status = m_instrumentsService->CreateFavoriteGroup(makeContext().get(), request, &reply);
//     // return ServiceReply::prepareServiceAnswer<CreateFavoriteGroupResponse>(status, reply);
//     return ServiceReply(grpc::Status::CANCELLED, "CreateFavoriteGroup method not available in current API version");
// }

// ServiceReply Instruments::DeleteFavoriteGroup(const std::string &favoriteId)
// {
//     // DeleteFavoriteGroupRequest has different fields in current API
//     // DeleteFavoriteGroupRequest request;
//     // request.set_favorite_id(favoriteId);
//     // DeleteFavoriteGroupResponse reply;
//     // Status status = m_instrumentsService->DeleteFavoriteGroup(makeContext().get(), request, &reply);
//     // return ServiceReply::prepareServiceAnswer<DeleteFavoriteGroupResponse>(status, reply);
//     return ServiceReply(grpc::Status::CANCELLED, "DeleteFavoriteGroup method not available in current API version");
// }

ServiceReply Instruments::GetFavoriteGroups()
{
    // GetFavoriteGroups method not available in current API
    // GetFavoriteGroupsRequest request;
    // GetFavoriteGroupsResponse reply;
    // Status status = m_instrumentsService->GetFavoriteGroups(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<GetFavoriteGroupsResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "GetFavoriteGroups method not available in current API version");
}

ServiceReply Instruments::GetCountries()
{
    GetCountriesRequest request;
    GetCountriesResponse reply;
    Status status = m_instrumentsService->GetCountries(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<GetCountriesResponse>(status, reply);
}

ServiceReply Instruments::FindInstrument(const std::string &query)
{
    FindInstrumentRequest request;
    request.set_query(query);
    FindInstrumentResponse reply;
    Status status = m_instrumentsService->FindInstrument(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<FindInstrumentResponse>(status, reply);
}

// ServiceReply Instruments::GetBrands(const std::string &instrumentId)
// {
//     // GetBrandsRequest has different fields in current API - uses paging instead of instrument_id
//     // GetBrandsRequest request;
//     // request.set_instrument_id(instrumentId);
//     // GetBrandsResponse reply;
//     // Status status = m_instrumentsService->GetBrands(makeContext().get(), request, &reply);
//     // return ServiceReply::prepareServiceAnswer<GetBrandsResponse>(status, reply);
//     return ServiceReply(grpc::Status::CANCELLED, "GetBrands method not available in current API version");
// }

ServiceReply Instruments::GetBrandBy(const std::string &brandId)
{
    // GetBrandBy uses GetBrandRequest with set_id (not set_brand_id)
    GetBrandRequest request;
    request.set_id(brandId);
    Brand reply;
    Status status = m_instrumentsService->GetBrandBy(makeContext().get(), request, &reply);
    return ServiceReply::prepareServiceAnswer<Brand>(status, reply);
}

// ServiceReply Instruments::GetAssetFundamentals(const std::string &instrumentId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
// {
//     // GetAssetFundamentalsRequest has different fields - uses assets repeated field instead of instrument_id and timestamps
//     // GetAssetFundamentalsRequest request;
//     // request.set_instrument_id(instrumentId);
//     // google::protobuf::Timestamp *from = new google::protobuf::Timestamp();
//     // from->set_seconds(fromseconds);
//     // from->set_nanos(fromnanos);
//     // request.set_allocated_from(from);
//     // google::protobuf::Timestamp *to = new google::protobuf::Timestamp();
//     // to->set_seconds(toseconds);
//     // to->set_nanos(tonanos);
//     // request.set_allocated_to(to);
//     // GetAssetFundamentalsResponse reply;
//     // Status status = m_instrumentsService->GetAssetFundamentals(makeContext().get(), request, &reply);
//     // return ServiceReply::prepareServiceAnswer<GetAssetFundamentalsResponse>(status, reply);
//     return ServiceReply(grpc::Status::CANCELLED, "GetAssetFundamentals method not available in current API version");
// }

// ServiceReply Instruments::GetAssetReports(const std::string &assetId, int64_t fromseconds, int32_t fromnanos, int64_t toseconds, int32_t tonanos)
// {
//     // GetAssetReportsRequest has different fields in current API
//     // GetAssetReportsRequest request;
//     // request.set_asset_id(assetId);
//     // google::protobuf::Timestamp *from = new google::protobuf::Timestamp();
//     // from->set_seconds(fromseconds);
//     // from->set_nanos(fromnanos);
//     // request.set_allocated_from(from);
//     // google::protobuf::Timestamp *to = new google::protobuf::Timestamp();
//     // to->set_seconds(toseconds);
//     // to->set_nanos(tonanos);
//     // request.set_allocated_to(to);
//     // GetAssetReportsResponse reply;
//     // Status status = m_instrumentsService->GetAssetReports(makeContext().get(), request, &reply);
//     // return ServiceReply::prepareServiceAnswer<GetAssetReportsResponse>(status, reply);
//     return ServiceReply(grpc::Status::CANCELLED, "GetAssetReports method not available in current API version");
// }

// GetConsensusForecasts method removed - does not exist in current API
// ServiceReply Instruments::GetConsensusForecasts(const std::string &instrumentId)
// {
//     GetConsensusForecastsRequest request;
//     GetConsensusForecastsResponse reply;
//     Status status = m_instrumentsService->GetConsensusForecasts(makeContext().get(), request, &reply);
//     return ServiceReply::prepareServiceAnswer<GetConsensusForecastsResponse>(status, reply);
// }

ServiceReply Instruments::GetForecastBy(const std::string &instrumentId)
{
    // GetForecastBy method not available in current API
    // GetForecastRequest request;
    // request.set_instrument_id(instrumentId);
    // GetForecastResponse reply;
    // Status status = m_instrumentsService->GetForecastBy(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<GetForecastResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "GetForecastBy method not available in current API version");
}

ServiceReply Instruments::GetRiskRates(const std::string &instrumentId)
{
    // GetRiskRates method not available in current API
    // RiskRatesRequest request;
    // request.add_instrument_id(instrumentId);
    // RiskRatesResponse reply;
    // Status status = m_instrumentsService->GetRiskRates(makeContext().get(), request, &reply);
    // return ServiceReply::prepareServiceAnswer<RiskRatesResponse>(status, reply);
    return ServiceReply(grpc::Status::CANCELLED, "GetRiskRates method not available in current API version");
}
