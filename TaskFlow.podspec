Pod::Spec.new do |s|
  s.name             = 'TaskFlow'
  s.version          = '0.1.0'
  s.summary          = 'A lightweight Swift-native task orchestration library that models work as a dependency graph.'
  s.description      = <<-DESC
TaskFlow models your work as a dependency graph. Each task runs only after its
dependencies complete, shared dependencies run exactly once, and independent
tasks execute in parallel. Built on Swift Concurrency (actors, structured
concurrency) and Combine.
                       DESC

  s.homepage         = 'https://github.com/chdaliu/task-flow'
  s.license          = { :type => 'MIT', :file => 'LICENSE' }
  s.author           = { 'Changda Liu' => 'chdaliu' }

  s.source           = { :git => 'https://github.com/chdaliu/task-flow.git', :tag => s.version.to_s }

  s.ios.deployment_target = '15.0'
  s.macos.deployment_target = '13.0'
  s.swift_version    = '6.0'

  s.source_files     = 'Sources/TaskFlow/**/*.swift'
  s.frameworks       = 'Foundation', 'Combine'
end
