workspace "bitlog"
    location "build"
    configurations { "Debug", "Release" }

project "bitlog-node"
    kind "ConsoleApp"
    language "C++"
    targetdir "build/%{cfg.buildcfg}"

    files { "src/apps/bitlog-node/**.cpp" }

    filter "configurations:Debug"
        defines { "DEBUG" }
        symbols "On"

    filter "configurations:Release"
        defines { "NDEBUG" }
        optimize "On"
