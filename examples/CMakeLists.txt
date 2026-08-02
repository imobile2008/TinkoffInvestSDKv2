foreach(example accounts quotes_stream sandbox_trade coro_trade backtest paper_trade)
  add_executable(${example} ${example}.cpp)
  target_link_libraries(${example} PRIVATE tinvest)
endforeach()
