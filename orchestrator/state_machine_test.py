from state_machine import StateMachine, State

def test_state_machine():
    # Инициализируем стейт-машину
    sm = StateMachine()

    # Сценарий 1: Тест допустимых переходов
    print(f"Initial state: {sm.get_current_state()}")
    assert sm.transition(State.init_startup), "Transition to init_startup failed"
    print(f"State after init_startup: {sm.get_current_state()}")
    
    assert sm.transition(State.in_startup_processing), "Transition to in_startup_processing failed"
    print(f"State after in_startup_processing: {sm.get_current_state()}")
    
    assert sm.transition(State.active), "Transition to active failed"
    print(f"State after active: {sm.get_current_state()}")
    
    assert sm.transition(State.init_shutdown), "Transition to init_shutdown failed"
    print(f"State after init_shutdown: {sm.get_current_state()}")
    
    assert sm.transition(State.in_shutdown_processing), "Transition to in_shutdown_processing failed"
    print(f"State after in_shutdown_processing: {sm.get_current_state()}")
    
    assert sm.transition(State.inactive), "Transition to inactive failed"
    print(f"State after inactive: {sm.get_current_state()}")

    # Сценарий 2: Тест недопустимых переходов
    sm = StateMachine()  # Возвращаем в начальное состояние
    print(f"Reset to initial state: {sm.get_current_state()}")

    assert not sm.transition(State.active), "Invalid transition to active succeeded"
    print(f"State after invalid transition attempt: {sm.get_current_state()}")

    assert not sm.transition(State.in_shutdown_processing), "Invalid transition to in_shutdown_processing succeeded"
    print(f"State after invalid transition attempt: {sm.get_current_state()}")

if __name__ == "__main__":
    test_state_machine()
    print("All tests passed!")
