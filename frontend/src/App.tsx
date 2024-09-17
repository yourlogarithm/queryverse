import { useState } from 'react';
import { Input } from 'antd';

const App = () => {
  const [loading, setLoading] = useState(false);
  const { Search } = Input;

  const onSearch = (value: string) => {
    setLoading(true);
    setTimeout(() => {
      setLoading(false);
    }, 1000);
  }

  return (<Search
    placeholder="Something you're looking for"
    enterButton="Search"
    size="large"
    loading={loading}
    onSearch={onSearch}
  />);
}


export default App;
