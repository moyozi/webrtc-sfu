import SwiftUI

struct ContentView: View {
    @StateObject private var viewModel = SfuViewModel()

    var body: some View {
        Group {
            if viewModel.inCall {
                CallView(viewModel: viewModel)
            } else {
                JoinView(viewModel: viewModel)
            }
        }
    }
}

#Preview {
    ContentView()
}
