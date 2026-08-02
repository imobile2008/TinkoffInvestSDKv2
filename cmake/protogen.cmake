# Locates protobuf/gRPC and provides tinvest_generate_protos().
#
# Works with either:
#   * gRPC installed with CMake config files (vcpkg, self-built) -> gRPC::grpc++
#   * distro packages (Ubuntu libgrpc++-dev) -> pkg-config grpc++

find_package(Threads REQUIRED)
# Prefer the CONFIG package (self-built protobuf in CMAKE_PREFIX_PATH wins
# over the distro's FindProtobuf module); fall back to the module otherwise.
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)  # protobuf::libprotobuf, protobuf::protoc
endif()

find_package(gRPC CONFIG QUIET)
if(gRPC_FOUND)
  set(TINVEST_GRPC_LIB gRPC::grpc++)
  set(TINVEST_GRPC_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
  set(TINVEST_GRPC_LIB PkgConfig::GRPCPP)
  find_program(TINVEST_GRPC_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

if(TARGET protobuf::protoc)
  set(TINVEST_PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
  find_program(TINVEST_PROTOC protoc REQUIRED)
endif()

# tinvest_generate_protos(<target> <list of .proto paths relative to source dir>)
#
# Creates a static library <target> with the generated *.pb.cc / *.grpc.pb.cc.
# Generated headers land in ${CMAKE_BINARY_DIR}/gen preserving the path relative
# to the contracts/ root, so protos importing each other by bare name work.
function(tinvest_generate_protos target protos)
  set(proto_root ${CMAKE_CURRENT_SOURCE_DIR}/contracts)
  set(gen_dir ${CMAKE_BINARY_DIR}/gen)
  file(MAKE_DIRECTORY ${gen_dir})

  set(gen_srcs "")
  foreach(proto IN LISTS protos)
    get_filename_component(abs ${proto} ABSOLUTE)
    file(RELATIVE_PATH rel ${proto_root} ${abs})
    get_filename_component(rel_dir ${rel} DIRECTORY)
    get_filename_component(stem ${rel} NAME_WE)
    if(rel_dir)
      set(out_base ${gen_dir}/${rel_dir}/${stem})
    else()
      set(out_base ${gen_dir}/${stem})
    endif()
    set(outs
      ${out_base}.pb.cc ${out_base}.pb.h
      ${out_base}.grpc.pb.cc ${out_base}.grpc.pb.h
    )
    # The proto package contains the C++ keyword `public`
    # (tinkoff.public.invest.api.contract.v1). protoc <= 3.21 emits it
    # verbatim into namespaces, producing invalid C++. Patch identifiers
    # only — descriptor/method-path strings ("tinkoff.public.invest...")
    # are untouched, so the wire protocol is unchanged.
    add_custom_command(
      OUTPUT ${outs}
      COMMAND ${TINVEST_PROTOC}
        --proto_path=${proto_root}
        --cpp_out=${gen_dir}
        --grpc_out=${gen_dir}
        --plugin=protoc-gen-grpc=${TINVEST_GRPC_PLUGIN}
        ${abs}
      COMMAND sed -i
        -e "s/namespace public {/namespace public_ {/g"
        -e "s/public::/public_::/g"
        -e "s|// namespace public|// namespace public_|g"
        ${outs}
      DEPENDS ${abs}
      COMMENT "protoc ${rel}"
      VERBATIM
    )
    list(APPEND gen_srcs ${out_base}.pb.cc ${out_base}.grpc.pb.cc)
  endforeach()

  add_library(${target} STATIC ${gen_srcs})
  target_include_directories(${target} SYSTEM PUBLIC ${gen_dir})
  target_link_libraries(${target} PUBLIC
    ${TINVEST_GRPC_LIB} protobuf::libprotobuf Threads::Threads
  )
endfunction()
