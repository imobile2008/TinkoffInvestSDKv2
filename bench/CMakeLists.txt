foreach(tool bench soak)
  add_executable(${tool} ${tool}.cpp)
  target_link_libraries(${tool} PRIVATE tinvest)
endforeach()
