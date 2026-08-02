#pragma once

// Message-only protobuf includes and the `tinvest::pb` namespace alias.
//
// The proto package `tinkoff.public.invest.api.contract.v1` contains the
// C++ keyword `public`; the build patches generated identifiers to
// `public_` (see cmake/protogen.cmake), hence the namespace below.

#include "common.pb.h"

namespace tinvest {
namespace pb = ::tinkoff::public_::invest::api::contract::v1;
}  // namespace tinvest
