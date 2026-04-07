import './App.css'
import Dashboard from './components/Dashboard'
import { Route, Switch } from "wouter"

function App() {

  return (
    <>
    <Switch>
      <Route path="/" component={Dashboard}></Route>
      <Route path="/Funds">Funds</Route>
      <Route path="/Holdings">Holdings</Route>
      <Route path="/Exceptions">Exceptions</Route>
      <Route path="/Upload">Upload</Route>
      <Route path="/Audit">Audit</Route>
    </Switch>
     
    </>
  )
}

export default App
