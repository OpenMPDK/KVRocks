set(TOOLS
  ldb.cc
  )
foreach(src ${TOOLS})
  get_filename_component(exename ${src} NAME_WE)
  add_executable(${exename}
    ${src})
  target_link_libraries(${exename}  ${LINK_KVDB} ${LINK_INSDB_STATIC})
  list(APPEND tool_deps ${exename})
endforeach()
add_custom_target(tools
  DEPENDS ${tool_deps})
#add_custom_command(TARGET ldb POST_BUILD
#COMMAND cp ldb /usr/bin)

