from enum import Enum

class State(str, Enum):
    init_startup = "init_startup"
    in_startup_processing = "in_startup_processing"
    active = "active"
    init_shutdown = "init_shutdown"
    in_shutdown_processing = "in_shutdown_processing"
    inactive = "inactive"

class StateMachine:
    def __init__(self):
        self.current_state = State.inactive

    def transition(self, new_state: State):
        transitions = {
            State.inactive: [State.init_startup],
            State.init_startup: [State.in_startup_processing],
            State.in_startup_processing: [State.active],
            State.active: [State.init_shutdown],
            State.init_shutdown: [State.in_shutdown_processing],
            State.in_shutdown_processing: [State.inactive]
        }

        # Проверка допустимости перехода
        if new_state in transitions[self.current_state]:
            self.current_state = new_state
            return True
        else:
            return False

    def get_current_state(self):
        return self.current_state
